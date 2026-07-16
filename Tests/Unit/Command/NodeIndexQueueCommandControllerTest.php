<?php

declare(strict_types=1);

namespace CodeQ\Meilisearch\QueueIndexer\Tests\Unit\Command;

use CodeQ\Meilisearch\QueueIndexer\AbstractIndexingJob;
use CodeQ\Meilisearch\QueueIndexer\Command\NodeIndexQueueCommandController;
use CodeQ\Meilisearch\QueueIndexer\IndexingJob;
use Flowpack\JobQueue\Common\Job\JobInterface;
use Flowpack\JobQueue\Common\Job\JobManager;
use Neos\ContentRepository\Domain\Model\Node;
use Neos\ContentRepository\Domain\Model\NodeData;
use Neos\ContentRepository\Domain\Model\NodeInterface;
use Neos\ContentRepository\Domain\Model\NodeType;
use Neos\ContentRepository\Domain\Model\Workspace;
use Neos\ContentRepository\Domain\Projection\Content\TraversableNodes;
use Neos\ContentRepository\Domain\Service\ContentDimensionCombinator;
use Neos\ContentRepository\Domain\Service\Context;
use Neos\ContentRepository\Domain\Service\ContextFactoryInterface;
use Neos\Flow\Persistence\PersistenceManagerInterface;
use PHPUnit\Framework\TestCase;

class NodeIndexQueueCommandControllerTest extends TestCase
{
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
