<?php

declare(strict_types=1);

namespace CodeQ\Meilisearch\QueueIndexer\Tests\Functional;

use CodeQ\Meilisearch\QueueIndexer\Indexer\NodeIndexer as QueuedNodeIndexer;
use CodeQ\Meilisearch\QueueIndexer\Command\NodeIndexQueueCommandController;
use CodeQ\Meilisearch\QueueIndexer\IndexingJob;
use Flowpack\JobQueue\Common\Job\JobInterface;
use Flowpack\JobQueue\Common\Job\JobManager;
use Flowpack\JobQueue\Common\Queue\Message;
use Flowpack\JobQueue\Common\Queue\QueueInterface;
use Medienreaktor\Meilisearch\Tests\Functional\RuntimeIndexingTest;
use Neos\ContentRepository\Search\Indexer\NodeIndexingManager;
use Neos\ContentRepository\Domain\Repository\NodeDataRepository;
use Medienreaktor\Meilisearch\Domain\Service\ScheduledVisibilityReconciliationService;

class QueuedRuntimeIndexingTest extends RuntimeIndexingTest
{
    /** @var string[] */
    private array $serializedJobs = [];

    private bool $deferExecution = false;

    protected function configureIndexer(): void
    {
        $queueIndexer = $this->objectManager->get(QueuedNodeIndexer::class);
        // Also isolate the decorator's legitimate synchronous fallback boundary.
        $this->inject($queueIndexer, 'indexClient', $this->index);
        $jobs = $this->createMock(JobManager::class);
        $jobs->method('queue')->willReturnCallback(function (string $queue, JobInterface $job): void {
            $serialized = serialize($job);
            // Actual Flow proxies must serialize only immutable job data, not injected services.
            self::assertStringNotContainsString('nodeIndexer', $serialized);
            self::assertStringNotContainsString('nodeDataRepository', $serialized);
            $this->serializedJobs[] = $serialized;
        });
        $this->inject($queueIndexer, 'jobManager', $jobs);
        $this->inject($queueIndexer, 'enableLiveAsyncIndexing', true);
        $this->inject($this->objectManager->get(NodeIndexingManager::class), 'nodeIndexer', $queueIndexer);
        $this->inject($this->repairs, 'nodeIndexer', $queueIndexer);
        $this->inject($this->objectManager->get(ScheduledVisibilityReconciliationService::class), 'nodeIndexer', $queueIndexer);
    }

    protected function executeDeferredJobs(): void
    {
        if ($this->deferExecution) {
            return;
        }
        $jobs = $this->serializedJobs;
        $this->serializedJobs = [];
        foreach ($jobs as $serialized) {
            $job = unserialize($serialized);
            self::assertInstanceOf(JobInterface::class, $job);
            self::assertTrue($job->execute($this->createMock(QueueInterface::class), new Message('test-message', new \ArrayObject())));
        }
    }

    public function testDelayedMoveRepairsTheOldRootEvenAfterContentNodeDataIsGone(): void
    {
        $a = $this->document('a');
        $b = $this->document('b');
        $content = $a->createNode('text', $this->types->getNodeType('Medienreaktor.Meilisearch.Testing:Content'));
        $content->setProperty('text', 'Deleted after enqueue');
        $this->persistChanges();
        $this->index->calls = [];
        $this->deferExecution = true;
        $draft = $this->context('user-test');
        $moving = $draft->getNodeByIdentifier($content->getIdentifier());
        $moving->moveInto($draft->getNodeByIdentifier($b->getIdentifier()));
        $this->draft->publishNode($moving, $this->live);
        $this->persistChanges();
        // Simulate a later deletion that physically removes NodeData before the worker runs.
        $this->objectManager->get(NodeDataRepository::class)->remove($moving->getNodeData());
        $this->persistChanges();
        $this->deferExecution = false;
        $this->executeDeferredJobs();
        foreach ([$a, $b] as $root) {
            $variants = $this->variantsWritten($root->getIdentifier());
            self::assertCount(2, $variants);
            foreach ($variants as $variant) {
                self::assertStringNotContainsString('Deleted after enqueue', json_encode($variant));
            }
        }
    }

    public function testWorkerReadsFreshEntitiesAfterChangesFromAnotherRequest(): void
    {
        $node = $this->document('document');
        $node->setProperty('title', 'First snapshot');
        $this->persistChanges();
        $this->index->calls = [];
        $connection = $this->objectManager->get(\Doctrine\ORM\EntityManagerInterface::class)->getConnection();
        // An external request changes the DB without updating this worker's identity map.
        $connection->executeStatement('UPDATE neos_contentrepository_domain_model_nodedata SET properties = ? WHERE identifier = ?', [json_encode(['title' => 'Later external snapshot']), $node->getIdentifier()]);
        $controller = $this->objectManager->get(NodeIndexQueueCommandController::class);
        $reset = new \ReflectionMethod(NodeIndexQueueCommandController::class, 'resetReadSnapshot');
        $reset->setAccessible(true);
        $reset->invoke($controller);
        $job = new IndexingJob('live', ['documentAggregateIdentifier' => $node->getIdentifier(), 'dimensionCombinations' => [['language' => ['en']]]]);
        self::assertTrue($job->execute($this->createMock(QueueInterface::class), new Message('test', new \ArrayObject())));
        $variants = $this->variantsWritten($node->getIdentifier());
        self::assertCount(1, $variants);
        self::assertSame('Later external snapshot', reset($variants)['title']);
    }

    protected function tearDown(): void
    {
        $this->inject($this->objectManager->get(NodeIndexingManager::class), 'nodeIndexer', $this->indexer);
        $this->inject($this->repairs, 'nodeIndexer', $this->indexer);
        $this->inject($this->objectManager->get(ScheduledVisibilityReconciliationService::class), 'nodeIndexer', $this->indexer);
        parent::tearDown();
    }
}
