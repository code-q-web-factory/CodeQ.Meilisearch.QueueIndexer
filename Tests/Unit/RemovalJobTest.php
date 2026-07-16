<?php

declare(strict_types=1);

namespace CodeQ\Meilisearch\QueueIndexer\Tests\Unit;

use CodeQ\Meilisearch\QueueIndexer\AbstractIndexingJob;
use CodeQ\Meilisearch\QueueIndexer\RemovalJob;
use Flowpack\JobQueue\Common\Queue\Message;
use Flowpack\JobQueue\Common\Queue\QueueInterface;
use Medienreaktor\Meilisearch\Indexer\NodeIndexer;
use Neos\ContentRepository\Domain\Repository\NodeDataRepository;
use PHPUnit\Framework\TestCase;

class RemovalJobTest extends TestCase
{
    public function testRemovalDoesNotNeedNodeDataWhenDocumentIdentifierWasPersisted(): void
    {
        $nodeIndexer = $this->createMock(NodeIndexer::class);
        $nodeIndexer->expects(self::once())
            ->method('removeDocumentByIdentifier')
            ->with('document-aggregate_language-de-hash');

        $nodeDataRepository = $this->createMock(NodeDataRepository::class);
        $nodeDataRepository->expects(self::never())->method('findByIdentifier');

        $queuedJob = new RemovalJob(null, [
            'documentIdentifier' => 'document-aggregate_language-de-hash',
        ]);
        $job = unserialize(serialize($queuedJob));
        self::assertInstanceOf(RemovalJob::class, $job);
        $this->inject($job, 'nodeIndexer', $nodeIndexer);
        $this->inject($job, 'nodeDataRepository', $nodeDataRepository);

        $result = $job->execute(
            $this->createMock(QueueInterface::class),
            new Message('message-id', new \ArrayObject())
        );

        self::assertTrue($result);
    }

    private function inject(AbstractIndexingJob $job, string $propertyName, object $value): void
    {
        $property = new \ReflectionProperty(AbstractIndexingJob::class, $propertyName);
        $property->setAccessible(true);
        $property->setValue($job, $value);
    }
}
