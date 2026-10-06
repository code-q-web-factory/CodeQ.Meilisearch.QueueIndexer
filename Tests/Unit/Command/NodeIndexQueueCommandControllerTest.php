<?php

declare(strict_types=1);

namespace CodeQ\Meilisearch\QueueIndexer\Tests\Unit\Command;

use CodeQ\Meilisearch\QueueIndexer\AbstractIndexingJob;
use CodeQ\Meilisearch\QueueIndexer\Command\NodeIndexQueueCommandController;
use CodeQ\Meilisearch\QueueIndexer\IndexingJob;
use Flowpack\JobQueue\Common\Exception as JobQueueException;
use Flowpack\JobQueue\Common\Job\JobInterface;
use Flowpack\JobQueue\Common\Job\JobManager;
use Flowpack\JobQueue\Common\Queue\Message;
use Flowpack\JobQueue\Common\Queue\QueueInterface;
use Flowpack\JobQueue\Common\Queue\QueueManager;
use Neos\ContentRepository\Domain\Factory\NodeFactory;
use Neos\ContentRepository\Domain\Model\Node;
use Neos\ContentRepository\Domain\Model\NodeData;
use Neos\ContentRepository\Domain\Model\NodeInterface;
use Neos\ContentRepository\Domain\Model\NodeType;
use Neos\ContentRepository\Domain\Model\Workspace;
use Neos\ContentRepository\Domain\Projection\Content\TraversableNodes;
use Neos\ContentRepository\Domain\Service\ContentDimensionCombinator;
use Neos\ContentRepository\Domain\Service\Context;
use Neos\ContentRepository\Domain\Service\ContextFactoryInterface;
use Neos\Flow\Cli\Exception\StopCommandException;
use Neos\Flow\Cli\Response;
use Neos\Flow\Log\ThrowableStorageInterface;
use Neos\Flow\Persistence\PersistenceManagerInterface;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;

class NodeIndexQueueCommandControllerTest extends TestCase
{
    public function testWorkStoresTheStackTraceOnlyForTheFirstFailureOfAStreak(): void
    {
        $firstFailure = new \RuntimeException('first failure', 0, new \RuntimeException('first reason'));
        $secondFailure = new \RuntimeException('second failure', 0, new \RuntimeException('second reason'));

        $jobManager = $this->createMock(JobManager::class);
        $jobManager->expects(self::exactly(2))
            ->method('waitAndExecute')
            ->will(self::onConsecutiveCalls(self::throwException($firstFailure), self::throwException($secondFailure)));

        $throwableStorage = $this->createMock(ThrowableStorageInterface::class);
        $throwableStorage->expects(self::once())
            ->method('logThrowable')
            ->with(self::identicalTo($firstFailure))
            ->willReturn('first failure - See also: stored-trace.txt');

        $loggedErrors = [];
        $logger = $this->createMock(LoggerInterface::class);
        $logger->method('error')
            ->willReturnCallback(static function (string $message) use (&$loggedErrors): void {
                $loggedErrors[] = $message;
            });

        $controller = new class extends NodeIndexQueueCommandController {
            protected function outputLine(string $text = '', array $arguments = [])
            {
            }
        };
        $this->inject($controller, 'contextFactory', $this->createMock(ContextFactoryInterface::class));
        $this->inject($controller, 'nodeFactory', $this->createMock(NodeFactory::class));
        $this->inject($controller, 'persistenceManager', $this->createMock(PersistenceManagerInterface::class));
        $this->inject($controller, 'jobManager', $jobManager);
        $this->inject($controller, 'throwableStorage', $throwableStorage);
        $this->inject($controller, 'logger', $logger);
        $this->inject($controller, 'response', new Response());

        try {
            $controller->workCommand(null, 2);
            self::fail('The worker must quit once --limit is reached.');
        } catch (StopCommandException $exception) {
        }

        self::assertSame([
            'Meilisearch indexing job failed: first failure - See also: stored-trace.txt',
            'Meilisearch indexing job failed: second failure. Reason: second reason',
        ], $loggedErrors);
    }

    public function testWorkStopsOnAnEmptyQueueWhenExitWhenEmptyIsGiven(): void
    {
        $reserveTimeouts = [];
        $jobManager = $this->createMock(JobManager::class);
        $jobManager->expects(self::once())
            ->method('waitAndExecute')
            ->willReturnCallback(static function (string $queueName, $timeout) use (&$reserveTimeouts): ?Message {
                self::assertSame(NodeIndexQueueCommandController::LIVE_QUEUE_NAME, $queueName);
                $reserveTimeouts[] = $timeout;
                return null;
            });

        $controller = $this->createController();
        $this->inject($controller, 'jobManager', $jobManager);
        $this->inject($controller, 'queueManager', $this->mockQueueManager($this->mockQueue(0, 0, 2)));

        try {
            $controller->workCommand(null, null, false, true);
            self::fail('The worker must quit once the queue is empty.');
        } catch (StopCommandException $exception) {
        }

        // A null reserve timeout would make the worker block for the backend's own
        // default (60s for DoctrineQueue) before it could notice the empty queue.
        self::assertSame([1], $reserveTimeouts);
        self::assertContains(
            'Quitting due to --exit-when-empty: no job left to reserve. Queue left at ready: 0, reserved: 0, failed: 2',
            $controller->outputLines
        );
    }

    public function testWorkKeepsWaitingOnAnEmptyQueueWithoutExitWhenEmpty(): void
    {
        $reserveTimeouts = [];
        $executedMessage = new Message('42', 'payload');
        $returnValues = [null, $executedMessage];

        $jobManager = $this->createMock(JobManager::class);
        $jobManager->expects(self::exactly(2))
            ->method('waitAndExecute')
            ->willReturnCallback(static function (string $queueName, $timeout) use (&$reserveTimeouts, &$returnValues): ?Message {
                $reserveTimeouts[] = $timeout;
                return array_shift($returnValues);
            });

        $controller = $this->createController();
        $this->inject($controller, 'jobManager', $jobManager);
        $this->inject($controller, 'queueManager', $this->mockQueueManager($this->mockQueue(7, 0, 0)));

        try {
            $controller->workCommand(null, 1);
            self::fail('The worker must quit once --limit is reached.');
        } catch (StopCommandException $exception) {
        }

        self::assertSame([null, null], $reserveTimeouts);
        self::assertContains(
            'Quitting after 1 job execution due to --limit. Queue left at ready: 7, reserved: 0, failed: 0',
            $controller->outputLines
        );
    }

    public function testWorkDoesNotMistakeAFailedJobForAnEmptyQueue(): void
    {
        $returnValues = [new JobQueueException('job failed'), null];

        $jobManager = $this->createMock(JobManager::class);
        $jobManager->expects(self::exactly(2))
            ->method('waitAndExecute')
            ->willReturnCallback(static function () use (&$returnValues): ?Message {
                $next = array_shift($returnValues);
                if ($next instanceof \Throwable) {
                    throw $next;
                }
                return $next;
            });

        $controller = $this->createController();
        $this->inject($controller, 'jobManager', $jobManager);
        $this->inject($controller, 'queueManager', $this->mockQueueManager($this->mockQueue(0, 0, 1)));
        $this->inject($controller, 'throwableStorage', $this->createMock(ThrowableStorageInterface::class));
        $this->inject($controller, 'logger', $this->createMock(LoggerInterface::class));

        try {
            $controller->workCommand(null, null, false, true);
            self::fail('The worker must quit once the queue is empty.');
        } catch (StopCommandException $exception) {
        }

        self::assertContains(
            'Quitting due to --exit-when-empty: no job left to reserve. Queue left at ready: 0, reserved: 0, failed: 1',
            $controller->outputLines
        );
    }

    public function testStatusReportsEveryBackendStateTogetherWithTheTableItCounted(): void
    {
        $controller = $this->createController();
        $this->inject($controller, 'queueManager', $this->mockQueueManager($this->mockQueue(4, 1, 37), [
            'className' => 'Flowpack\\JobQueue\\Doctrine\\Queue\\DoctrineQueue',
            'options' => ['tableName' => 'codeq_meilisearch_queueindexer_live'],
        ]));

        $controller->statusCommand();

        self::assertSame([
            '<b>CodeQ.Meilisearch.QueueIndexer.Live</b>',
            '  Backend          : Flowpack\\JobQueue\\Doctrine\\Queue\\DoctrineQueue',
            '  Table            : codeq_meilisearch_queueindexer_live',
            '  state "ready"    : 4  waiting for a worker, including jobs released for a retry',
            '  state "reserved" : 1  currently held by a worker',
            '  state "failed"   : 37  given up after 4 attempts',
            '  rows in total    : 42',
        ], $controller->outputLines);
    }

    public function testStatusReportsAnUnreachableQueueInsteadOfFailingWithAStackTrace(): void
    {
        $queueManager = $this->createMock(QueueManager::class);
        $queueManager->method('getQueue')->willThrowException(
            new \RuntimeException('The queue table "codeq_meilisearch_queueindexer_live" could not be found.')
        );

        $controller = $this->createController();
        $this->inject($controller, 'queueManager', $queueManager);

        $controller->statusCommand();

        self::assertSame([
            '<b>CodeQ.Meilisearch.QueueIndexer.Live</b>',
            '  <error>Queue not available: The queue table "codeq_meilisearch_queueindexer_live" could not be found.</error>',
        ], $controller->outputLines);
    }

    public function testBuildEnqueuesEveryAllowedDimensionWithExplicitSnapshotSemantics(): void
    {
        $germanDimensions = ['language' => ['de']];
        $romanianDimensions = ['language' => ['ro_RO']];
        $germanNodeData = $this->createMock(NodeData::class);
        $romanianNodeData = $this->createMock(NodeData::class);
        $germanRoot = $this->createRootNode($germanDimensions, $germanNodeData, 'german-document');
        $romanianRoot = $this->createRootNode($romanianDimensions, $romanianNodeData, 'romanian-document');

        $germanContext = $this->createMock(Context::class);
        $germanContext->method('getRootNode')->willReturn($germanRoot);
        $romanianContext = $this->createMock(Context::class);
        $romanianContext->method('getRootNode')->willReturn($romanianRoot);

        $contextFactory = $this->createMock(ContextFactoryInterface::class);
        $contextFactory->expects(self::exactly(2))
            ->method('create')
            ->withConsecutive(
                [['workspaceName' => 'live', 'dimensions' => $germanDimensions]],
                [['workspaceName' => 'live', 'dimensions' => $romanianDimensions]]
            )
            ->willReturnOnConsecutiveCalls($germanContext, $romanianContext);

        $dimensionCombinator = $this->createMock(ContentDimensionCombinator::class);
        $dimensionCombinator->expects(self::once())
            ->method('getAllAllowedCombinations')
            ->willReturn([$germanDimensions, $romanianDimensions]);

        $persistenceManager = $this->createMock(PersistenceManagerInterface::class);
        $persistenceManager->method('getIdentifierByObject')
            ->willReturnMap([
                [$germanNodeData, 'german-persistence-id'],
                [$romanianNodeData, 'romanian-persistence-id'],
            ]);
        $persistenceManager->expects(self::exactly(2))->method('clearState');

        $jobs = [];
        $jobManager = $this->createMock(JobManager::class);
        $jobManager->expects(self::exactly(2))
            ->method('queue')
            ->willReturnCallback(static function (string $queueName, JobInterface $job) use (&$jobs): void {
                self::assertSame(NodeIndexQueueCommandController::LIVE_QUEUE_NAME, $queueName);
                self::assertInstanceOf(IndexingJob::class, $job);
                $jobs[] = $job;
            });

        $controller = new class extends NodeIndexQueueCommandController {
            protected function outputLine(string $text = '', array $arguments = [])
            {
            }
        };
        $this->inject($controller, 'contextFactory', $contextFactory);
        $this->inject($controller, 'contentDimensionCombinator', $dimensionCombinator);
        $this->inject($controller, 'persistenceManager', $persistenceManager);
        $this->inject($controller, 'jobManager', $jobManager);

        $controller->buildCommand();

        self::assertCount(2, $jobs);
        $this->assertSnapshotJob($jobs[0], $germanDimensions, 'german-persistence-id');
        $this->assertSnapshotJob($jobs[1], $romanianDimensions, 'romanian-persistence-id');
    }

    private function createRootNode(array $dimensions, NodeData $nodeData, string $identifier): NodeInterface
    {
        $nodeType = $this->createMock(NodeType::class);
        $nodeType->method('getConfiguration')->with('search')->willReturn([
            'fulltext' => ['isRoot' => true],
        ]);
        $nodeType->method('getName')->willReturn('Neos.Neos:Document');

        $context = $this->createMock(Context::class);
        $context->method('getDimensions')->willReturn($dimensions);
        $workspace = $this->createMock(Workspace::class);
        $workspace->method('getName')->willReturn('live');

        $node = $this->createMock(Node::class);
        $node->method('getNodeType')->willReturn($nodeType);
        $node->method('getNodeData')->willReturn($nodeData);
        $node->method('getIdentifier')->willReturn($identifier);
        $node->method('getContext')->willReturn($context);
        $node->method('getWorkspace')->willReturn($workspace);
        $node->method('getPath')->willReturn('/sites/example/' . $identifier);
        $node->method('findChildNodes')->willReturn(TraversableNodes::fromArray([]));

        return $node;
    }

    private function assertSnapshotJob(
        IndexingJob $job,
        array $expectedDimensions,
        string $expectedPersistenceIdentifier
    ): void {
        $node = $this->readProperty($job, AbstractIndexingJob::class, 'node');
        self::assertSame($expectedPersistenceIdentifier, $node['persistenceObjectIdentifier']);
        self::assertSame($expectedDimensions, $node['dimensions']);
        self::assertFalse($this->readProperty($job, IndexingJob::class, 'indexAllDimensions'));
        self::assertFalse($this->readProperty($job, IndexingJob::class, 'indexFallbackDimensions'));
        self::assertSame(
            $expectedDimensions,
            $this->readProperty($job, IndexingJob::class, 'targetDimensionCombination')
        );
    }

    /**
     * A controller that records its output instead of writing to the console, with
     * a CLI response so quit() can set an exit code.
     */
    private function createController(): NodeIndexQueueCommandController
    {
        $controller = new class extends NodeIndexQueueCommandController {
            /** @var string[] */
            public array $outputLines = [];

            protected function outputLine(string $text = '', array $arguments = [])
            {
                $this->outputLines[] = $arguments === [] ? $text : vsprintf($text, $arguments);
            }
        };
        $this->inject($controller, 'contextFactory', $this->createMock(ContextFactoryInterface::class));
        $this->inject($controller, 'nodeFactory', $this->createMock(NodeFactory::class));
        $this->inject($controller, 'persistenceManager', $this->createMock(PersistenceManagerInterface::class));
        $this->inject($controller, 'response', new Response());

        return $controller;
    }

    private function mockQueue(int $ready, int $reserved, int $failed): QueueInterface
    {
        $queue = $this->createMock(QueueInterface::class);
        $queue->method('countReady')->willReturn($ready);
        $queue->method('countReserved')->willReturn($reserved);
        $queue->method('countFailed')->willReturn($failed);

        return $queue;
    }

    private function mockQueueManager(QueueInterface $queue, array $queueSettings = []): QueueManager
    {
        $queueManager = $this->createMock(QueueManager::class);
        $queueManager->method('getQueue')
            ->with(NodeIndexQueueCommandController::LIVE_QUEUE_NAME)
            ->willReturn($queue);
        $queueManager->method('getQueueSettings')
            ->with(NodeIndexQueueCommandController::LIVE_QUEUE_NAME)
            ->willReturn($queueSettings);

        return $queueManager;
    }

    private function readProperty(object $target, string $className, string $propertyName)
    {
        $property = new \ReflectionProperty($className, $propertyName);
        $property->setAccessible(true);
        return $property->getValue($target);
    }

    private function inject(object $target, string $propertyName, object $value): void
    {
        $property = new \ReflectionProperty(NodeIndexQueueCommandController::class, $propertyName);
        $property->setAccessible(true);
        $property->setValue($target, $value);
    }
}
