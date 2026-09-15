<?php

declare(strict_types=1);

namespace CodeQ\Meilisearch\QueueIndexer\Tests\Unit\Indexer;

use CodeQ\Meilisearch\QueueIndexer\AbstractIndexingJob;
use CodeQ\Meilisearch\QueueIndexer\Command\NodeIndexQueueCommandController;
use CodeQ\Meilisearch\QueueIndexer\Indexer\NodeIndexer;
use CodeQ\Meilisearch\QueueIndexer\RemovalJob;
use Flowpack\JobQueue\Common\Job\JobInterface;
use Flowpack\JobQueue\Common\Job\JobManager;
use Medienreaktor\Meilisearch\Domain\Service\DimensionsService;
use Medienreaktor\Meilisearch\Domain\Service\MeilisearchIndex;
use Medienreaktor\Meilisearch\Indexer\NodeIndexer as UpstreamNodeIndexer;
use Neos\ContentRepository\Domain\Model\Node;
use Neos\ContentRepository\Domain\Model\NodeData;
use Neos\ContentRepository\Domain\Model\NodeType;
use Neos\ContentRepository\Domain\Model\Workspace;
use Neos\ContentRepository\Domain\NodeAggregate\NodeAggregateIdentifier;
use Neos\ContentRepository\Domain\Service\Context;
use Neos\ContentRepository\Domain\Service\ContextFactoryInterface;
use Neos\Flow\Persistence\PersistenceManagerInterface;
use PHPUnit\Framework\TestCase;

class NodeIndexerTest extends TestCase
{
    public function testRemovalPayloadContainsTheFulltextRootAndAffectedDimensions(): void
    {
        $dimensionCombinations = [
            ['language' => ['de']],
            ['language' => ['de'], 'country' => ['at']],
        ];
        $nodeData = $this->createMock(NodeData::class);
        $context = $this->createMock(Context::class);
        $context->method('getDimensions')->willReturn(['language' => ['de']]);
        $context->method('getWorkspaceName')->willReturn('live');
        $workspace = $this->createMock(Workspace::class);
        $workspace->method('getName')->willReturn('live');
        $contentNodeType = $this->createMock(NodeType::class);
        $contentNodeType->method('getName')->willReturn('Neos.Neos:Content');
        $contentNodeType->method('hasConfiguration')->with('search')->willReturn(false);
        $documentNodeType = $this->createMock(NodeType::class);
        $documentNodeType->method('hasConfiguration')->with('search')->willReturn(true);
        $documentNodeType->method('getConfiguration')->with('search')->willReturn([
            'fulltext' => ['isRoot' => true],
        ]);

        $fulltextRoot = $this->createMock(Node::class);
        $fulltextRoot->method('getNodeType')->willReturn($documentNodeType);
        $fulltextRoot->method('getNodeAggregateIdentifier')->willReturn(
            NodeAggregateIdentifier::fromString('document-aggregate')
        );

        $node = $this->createMock(Node::class);
        $node->method('getNodeData')->willReturn($nodeData);
        $node->method('getIdentifier')->willReturn('content-aggregate');
        $node->method('getNodeAggregateIdentifier')->willReturn(
            NodeAggregateIdentifier::fromString('content-aggregate')
        );
        $node->method('getContext')->willReturn($context);
        $node->method('getWorkspace')->willReturn($workspace);
        $node->method('getNodeType')->willReturn($contentNodeType);
        $node->method('getPath')->willReturn('/sites/example/document/content');
        $node->method('findParentNode')->willReturn($fulltextRoot);

        $persistenceManager = $this->createMock(PersistenceManagerInterface::class);
        $persistenceManager->method('getIdentifierByObject')->with($nodeData)->willReturn('persistence-id');
        $dimensionsService = $this->createMock(DimensionsService::class);
        $dimensionsService->expects(self::once())
            ->method('getDimensionCombinationsForIndexing')
            ->with($fulltextRoot)
            ->willReturn($dimensionCombinations);
        $jobManager = $this->createMock(JobManager::class);
        $jobManager->expects(self::once())
            ->method('queue')
            ->with(
                NodeIndexQueueCommandController::LIVE_QUEUE_NAME,
                self::callback(function (JobInterface $job): bool {
                    self::assertInstanceOf(RemovalJob::class, $job);
                    $nodeProperty = new \ReflectionProperty(AbstractIndexingJob::class, 'node');
                    $nodeProperty->setAccessible(true);
                    $payload = $nodeProperty->getValue($job);
                    self::assertIsArray($payload);
                    self::assertArrayNotHasKey('documentIdentifier', $payload);
                    self::assertSame('document-aggregate', $payload['documentAggregateIdentifier']);
                    self::assertSame(
                        [
                            ['language' => ['de']],
                            ['language' => ['de'], 'country' => ['at']],
                        ],
                        $payload['dimensionCombinations']
                    );
                    self::assertSame('persistence-id', $payload['persistenceObjectIdentifier']);
                    self::assertSame(['language' => ['de']], $payload['dimensions']);
                    return true;
                })
            );

        $nodeIndexer = new NodeIndexer();
        $this->inject($nodeIndexer, NodeIndexer::class, 'persistenceManager', $persistenceManager);
        $this->inject($nodeIndexer, NodeIndexer::class, 'jobManager', $jobManager);
        $this->inject($nodeIndexer, UpstreamNodeIndexer::class, 'dimensionsService', $dimensionsService);

        $nodeIndexer->removeNode($node);
    }

    public function testSynchronousFallbackForwardsTargetDimensionsToCombinedFixes(): void
    {
        $targetDimensions = ['language' => ['en']];
        $nodeType = $this->createMock(NodeType::class);
        $nodeType->method('hasConfiguration')->with('search')->willReturn(true);
        $nodeType->method('getConfiguration')->with('search')->willReturn([
            'fulltext' => ['isRoot' => true],
        ]);

        $node = $this->createMock(Node::class);
        $node->method('getNodeType')->willReturn($nodeType);
        $node->method('isVisible')->willReturn(true);
        $node->method('getNodeAggregateIdentifier')->willReturn(
            NodeAggregateIdentifier::fromString('document-aggregate')
        );

        $dimensionsService = $this->createMock(DimensionsService::class);
        $dimensionsService->method('getDimensionCombinationsForIndexing')->with($node)->willReturn([]);
        $dimensionsService->expects(self::once())
            ->method('hash')
            ->with($targetDimensions)
            ->willReturn('language-en-hash');

        $indexClient = $this->createMock(MeilisearchIndex::class);
        $indexClient->expects(self::once())
            ->method('deleteDocuments')
            ->with(['document-aggregate_language-en-hash']);
        $indexClient->expects(self::never())->method('addDocuments');

        $variantContext = $this->createMock(Context::class);
        $variantContext->method('getNodeByIdentifier')->with('document-aggregate')->willReturn(null);
        $contextFactory = $this->createMock(ContextFactoryInterface::class);
        $contextFactory->expects(self::once())
            ->method('create')
            ->with(['workspaceName' => 'live', 'dimensions' => $targetDimensions])
            ->willReturn($variantContext);

        $nodeIndexer = new NodeIndexer();
        $this->inject($nodeIndexer, UpstreamNodeIndexer::class, 'dimensionsService', $dimensionsService);
        $this->inject($nodeIndexer, UpstreamNodeIndexer::class, 'indexClient', $indexClient);
        $this->inject($nodeIndexer, UpstreamNodeIndexer::class, 'contextFactory', $contextFactory);

        $method = new \ReflectionMethod(NodeIndexer::class, 'indexSynchronously');
        $method->setAccessible(true);
        $method->invoke($nodeIndexer, $node, null, false, false, $targetDimensions);
        $nodeIndexer->flush();
    }

    private function inject(object $target, string $className, string $propertyName, object $value): void
    {
        $property = new \ReflectionProperty($className, $propertyName);
        $property->setAccessible(true);
        $property->setValue($target, $value);
    }
}
