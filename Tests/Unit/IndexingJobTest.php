<?php

declare(strict_types=1);

namespace CodeQ\Meilisearch\QueueIndexer\Tests\Unit;

use CodeQ\Meilisearch\QueueIndexer\AbstractIndexingJob;
use CodeQ\Meilisearch\QueueIndexer\IndexingJob;
use Flowpack\JobQueue\Common\Queue\Message;
use Flowpack\JobQueue\Common\Queue\QueueInterface;
use Medienreaktor\Meilisearch\Indexer\NodeIndexer;
use Neos\ContentRepository\Domain\Factory\NodeFactory;
use Neos\ContentRepository\Domain\Model\NodeData;
use Neos\ContentRepository\Domain\Model\NodeInterface;
use Neos\ContentRepository\Domain\Model\Workspace;
use Neos\ContentRepository\Domain\Repository\NodeDataRepository;
use Neos\ContentRepository\Domain\Service\Context;
use Neos\ContentRepository\Domain\Service\ContextFactoryInterface;
use PHPUnit\Framework\TestCase;

class IndexingJobTest extends TestCase
{
    public function testSnapshotJobForwardsTargetDimensionInsteadOfFallbackNodeDimension(): void
    {
        $targetDimensions = ['language' => ['de_CH']];
        $workspace = $this->createMock(Workspace::class);
        $workspace->method('getName')->willReturn('live');
        $nodeData = $this->createMock(NodeData::class);
        $nodeData->method('getWorkspace')->willReturn($workspace);
        $node = $this->createMock(NodeInterface::class);
        $context = $this->createMock(Context::class);

        $nodeDataRepository = $this->createMock(NodeDataRepository::class);
        $nodeDataRepository->expects(self::once())
            ->method('findByIdentifier')
            ->with('romanian-persistence-id')
            ->willReturn($nodeData);
        $contextFactory = $this->createMock(ContextFactoryInterface::class);
        $contextFactory->expects(self::once())
            ->method('create')
            ->with([
                'workspaceName' => 'live',
                'invisibleContentShown' => true,
                'inaccessibleContentShown' => false,
                'dimensions' => $targetDimensions,
            ])
            ->willReturn($context);
        $nodeFactory = $this->createMock(NodeFactory::class);
        $nodeFactory->expects(self::once())
            ->method('createFromNodeData')
            ->with($nodeData, $context)
            ->willReturn($node);
        $nodeIndexer = $this->createMock(NodeIndexer::class);
        $nodeIndexer->expects(self::once())
            ->method('indexNode')
            ->with($node, null, false, false, $targetDimensions);

        $job = new IndexingJob(
            null,
            [
                'persistenceObjectIdentifier' => 'romanian-persistence-id',
                'identifier' => 'romanian-document',
                'dimensions' => ['language' => ['de']],
            ],
            false,
            false,
            $targetDimensions
        );
        $this->inject($job, 'nodeDataRepository', $nodeDataRepository);
        $this->inject($job, 'contextFactory', $contextFactory);
        $this->inject($job, 'nodeFactory', $nodeFactory);
        $this->inject($job, 'nodeIndexer', $nodeIndexer);

        self::assertTrue($job->execute(
            $this->createMock(QueueInterface::class),
            $this->createMock(Message::class)
        ));
    }

    private function inject(object $target, string $propertyName, object $value): void
    {
        $property = new \ReflectionProperty(AbstractIndexingJob::class, $propertyName);
        $property->setAccessible(true);
        $property->setValue($target, $value);
    }
}
