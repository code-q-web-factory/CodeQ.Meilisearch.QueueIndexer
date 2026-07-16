<?php

declare(strict_types=1);

namespace CodeQ\Meilisearch\QueueIndexer;

use Flowpack\JobQueue\Common\Queue\Message;
use Flowpack\JobQueue\Common\Queue\QueueInterface;
use Neos\ContentRepository\Domain\Model\NodeData;
use Neos\ContentRepository\Domain\Model\NodeInterface;
use Neos\Flow\Log\Utility\LogEnvironment;

/**
 * Executes a single deferred `removeNode` call against Medienreaktor.Meilisearch.
 */
class RemovalJob extends AbstractIndexingJob
{
    public function execute(QueueInterface $queue, Message $message): bool
    {
        if (
            isset($this->node['documentIdentifier'])
            && is_string($this->node['documentIdentifier'])
            && $this->node['documentIdentifier'] !== ''
        ) {
            $this->nodeIndexer->removeDocumentByIdentifier($this->node['documentIdentifier']);
            return true;
        }

        // Compatibility path for jobs which were already queued before the
        // immutable document identifier was added to the payload.
        /** @var NodeData $nodeData */
        $nodeData = $this->nodeDataRepository->findByIdentifier($this->node['persistenceObjectIdentifier']);

        if (!$nodeData instanceof NodeData) {
            $this->logger->notice(
                sprintf(
                    'Legacy removal job for node %s has no document identifier and its NodeData is gone; skipping',
                    $this->node['identifier']
                ),
                LogEnvironment::fromMethodName(__METHOD__)
            );
            return true;
        }

        $context = $this->contextFactory->create([
            'workspaceName' => $this->targetWorkspaceName ?: $nodeData->getWorkspace()->getName(),
            'invisibleContentShown' => true,
            'removedContentShown' => true,
            'inaccessibleContentShown' => false,
            'dimensions' => $this->node['dimensions'],
        ]);

        $node = $this->nodeFactory->createFromNodeData($nodeData, $context);
        if (!$node instanceof NodeInterface) {
            $this->logger->info(
                sprintf('Node %s could not be rehydrated for removal', $this->node['identifier']),
                LogEnvironment::fromMethodName(__METHOD__)
            );
            return true;
        }

        $this->nodeIndexer->removeNode($node);
        return true;
    }

    public function getLabel(): string
    {
        return sprintf('Meilisearch Removal Job (%s)', $this->getIdentifier());
    }
}
