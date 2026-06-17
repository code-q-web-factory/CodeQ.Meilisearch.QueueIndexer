<?php

declare(strict_types=1);

namespace CodeQ\Meilisearch\QueueIndexer;

use Flowpack\JobQueue\Common\Queue\Message;
use Flowpack\JobQueue\Common\Queue\QueueInterface;
use Neos\ContentRepository\Domain\Model\NodeData;
use Neos\ContentRepository\Domain\Model\NodeInterface;
use Neos\Flow\Log\Utility\LogEnvironment;

/**
 * Executes a single deferred `indexNode` call against Medienreaktor.Meilisearch.
 */
class IndexingJob extends AbstractIndexingJob
{
    public function execute(QueueInterface $queue, Message $message): bool
    {
        /** @var NodeData $nodeData */
        $nodeData = $this->nodeDataRepository->findByIdentifier($this->node['persistenceObjectIdentifier']);
        if (!$nodeData instanceof NodeData) {
            // The underlying NodeData can disappear between enqueue and execute - e.g. the node was
            // deleted. Skip instead of failing the job; a RemovalJob covers the delete path.
            $this->logger->notice(
                sprintf('NodeData for node %s not found, skipping indexing job', $this->node['identifier']),
                LogEnvironment::fromMethodName(__METHOD__)
            );
            return true;
        }

        $context = $this->contextFactory->create([
            'workspaceName' => $this->targetWorkspaceName ?: $nodeData->getWorkspace()->getName(),
            'invisibleContentShown' => true,
            'inaccessibleContentShown' => false,
            'dimensions' => $this->node['dimensions'],
        ]);

        $node = $this->nodeFactory->createFromNodeData($nodeData, $context);
        if (!$node instanceof NodeInterface) {
            $this->logger->warning(
                sprintf('Node %s could not be rehydrated for indexing', $this->node['identifier']),
                LogEnvironment::fromMethodName(__METHOD__)
            );
            return true;
        }

        $this->nodeIndexer->indexNode($node, $this->targetWorkspaceName);
        return true;
    }

    public function getLabel(): string
    {
        return sprintf('Meilisearch Indexing Job (%s)', $this->getIdentifier());
    }
}
