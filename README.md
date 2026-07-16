# CodeQ.Meilisearch.QueueIndexer

Asynchronous live indexing for [Medienreaktor.Meilisearch](https://github.com/medienreaktor/Medienreaktor.Meilisearch) using Flowpack JobQueue.

The package decorates Neos' `NodeIndexerInterface` so publish operations enqueue live indexing jobs instead of writing to Meilisearch synchronously. A worker command drains the queue and delegates the actual indexing/removal work back to `Medienreaktor.Meilisearch`.

## Installation

Require the package in the Neos distribution:

```bash
composer require codeq/meilisearch-queueindexer
```

If the package is not available on Packagist yet, add the repository explicitly:

```json
{
    "repositories": {
        "codeq/meilisearch-queueindexer": {
            "type": "vcs",
            "url": "git@github.com:code-q-web-factory/CodeQ.Meilisearch.QueueIndexer.git"
        }
    }
}
```

This package expects `medienreaktor/meilisearch` to be installed and configured by the site package. Projects that use a forked Medienreaktor package must add the corresponding VCS repository in the root `composer.json`.

## Queue Configuration

The default configuration uses a Doctrine queue named `CodeQ.Meilisearch.QueueIndexer.Live`:

```yaml
Flowpack:
  JobQueue:
    Common:
      queues:
        'CodeQ.Meilisearch.QueueIndexer.Live':
          preset: 'CodeQ.Meilisearch.QueueIndexer.Live'
      presets:
        'CodeQ.Meilisearch.QueueIndexer.Live':
          className: 'Flowpack\JobQueue\Doctrine\Queue\DoctrineQueue'
          options:
            tableName: 'codeq_meilisearch_queueindexer_live'
```

Set up the queue after installing:

```bash
./flow queue:setup 'CodeQ.Meilisearch.QueueIndexer.Live'
```

## Commands

Drain the live indexing queue:

```bash
./flow nodeindexqueue:work --verbose
```

Enqueue all fulltext-root documents from the live workspace:

```bash
./flow nodeindexqueue:build --verbose
```

Inspect queue state:

```bash
./flow nodeindexqueue:status
```

Flush the live queue:

```bash
./flow nodeindexqueue:flush
```

## Settings

Live async indexing is enabled by default:

```yaml
CodeQ:
  Meilisearch:
    QueueIndexer:
      enableLiveAsyncIndexing: true
```

Set `enableLiveAsyncIndexing: false` to fall back to synchronous Medienreaktor indexing.

## Scheduled Visibility Reconciliation

`Medienreaktor.Meilisearch` keeps scheduled visibility reconciliation disabled
by default. When that upstream feature is enabled, its
`scheduledvisibility:reconcile` command delegates index and removal
operations to `NodeIndexerInterface`. This package's decorator therefore puts
those operations onto the live queue automatically; no second reconciliation
command or queue setting is required here.

The reconciliation command only submits jobs. Keep the worker running to apply
them:

```bash
./flow nodeindexqueue:work --verbose
```

The normal snapshot build remains unchanged. Enabling live asynchronous
indexing does not enable scheduled reconciliation by itself.

Removal jobs persist the exact Meilisearch document identifier when they are
enqueued. They can therefore delete the document even if the corresponding
Neos `NodeData` has already disappeared before a worker executes the job.

Jobs which were already queued by an older package version do not contain that
identifier and continue through the legacy node-rehydration path. Drain the
live queue before upgrading. If that cannot be guaranteed, perform a one-time
index flush and complete rebuild after deploying the new worker so previously
orphaned documents cannot remain searchable.

## Index Name Configuration

The queue indexer resolves its own `indexClient` from `Medienreaktor.Meilisearch.indexName`. For a complete environment-specific setup, the consuming site should also configure the upstream Medienreaktor indexer/commands/query builder to use the same setting, for example:

```yaml
Medienreaktor\Meilisearch\Indexer\NodeIndexer:
  properties:
    indexClient:
      object:
        factoryObjectName: Medienreaktor\Meilisearch\Factory\IndexFactory
        factoryMethodName: create
        arguments:
          1:
            setting: 'Medienreaktor.Meilisearch.indexName'
```

## Production Worker

Run `nodeindexqueue:work` continuously in production. For Beach-style deployments, wrap it in a restart loop so the worker comes back after PHP exits:

```bash
while true; do
  /application/flow nodeindexqueue:work --verbose
  sleep 10
done
```
