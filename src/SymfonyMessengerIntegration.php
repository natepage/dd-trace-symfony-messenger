<?php
declare(strict_types=1);

namespace NatePage\DDTrace;

use DDTrace\HookData;
use DDTrace\Integrations\Integration;
use DDTrace\SpanData;
use DDTrace\SpanLink;
use DDTrace\Tag;
use DDTrace\Util\ObjectKVStore;
use Symfony\Component\Messenger\Bridge\AmazonSqs\Transport\AmazonSqsReceivedStamp;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Stamp\BusNameStamp;
use Symfony\Component\Messenger\Stamp\DelayStamp;
use Symfony\Component\Messenger\Stamp\ReceivedStamp;
use Symfony\Component\Messenger\Stamp\RedeliveryStamp;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;

use function DDTrace\active_span;
use function DDTrace\close_span;
use function DDTrace\consume_distributed_tracing_headers;
use function DDTrace\generate_distributed_tracing_headers;
use function DDTrace\install_hook;
use function DDTrace\logs_correlation_trace_id;
use function DDTrace\set_distributed_tracing_context;
use function DDTrace\start_trace_span;
use function DDTrace\trace_method;

final class SymfonyMessengerIntegration extends Integration
{
    const NAME = 'symfonymessenger';

    public function init(): int
    {
        $integration = $this;

        // Attach current context to Envelope before sender sends it to remote queue
        install_hook(
            'Symfony\Component\Messenger\Transport\Sender\SenderInterface::send',
            function (HookData $hook) {
                /** @var \Symfony\Component\Messenger\Envelope $envelope */
                $envelope = $hook->args[0];

                if (\ddtrace_config_distributed_tracing_enabled()) {
                    $hook->overrideArguments([
                        $envelope->with(new DDTraceStamp(generate_distributed_tracing_headers()))
                    ]);
                }
            }
        );

        trace_method(
            'Symfony\Component\Messenger\Worker',
            'handleMessage',
            [
                'prehook' => function (SpanData $span, array $args) use ($integration, &$newTrace) {
                    /** @var \Symfony\Component\Messenger\Envelope $envelope */
                    $envelope = $args[0];

                    $integration->setSpanAttributes($span, 'symfony.messenger.handle_message', 'receive', $envelope);

                    $ddTraceStamp = $envelope->last(DDTraceStamp::class);
                    if ($ddTraceStamp instanceof DDTraceStamp) {
                        if (\dd_trace_env_config('DD_TRACE_SYMFONY_MESSENGER_DISTRIBUTED_TRACING')) {
                            $newTrace = start_trace_span();
                            $integration->setSpanAttributes($newTrace, 'symfony.messenger.handle_message', 'receive', $envelope);

                            consume_distributed_tracing_headers($ddTraceStamp->getHeaders());

                            $span->links[] = $newTrace->getLink();
                            $newTrace->links[] = $span->getLink();
                        } else {
                            $span->links[] = SpanLink::fromHeaders($ddTraceStamp->getHeaders());
                        }
                    }
                },
                'posthook' => function (SpanData $span, array $args, $returnValue, \Throwable $exception = null) use ($integration, &$newTrace) {
                    /** @var \Symfony\Component\Messenger\Envelope $envelope */
                    $envelope = $args[0];

                    if ($exception !== null) {
                        // Used by Logs Correlation to track the origin of an exception
                        ObjectKVStore::put(
                            $exception,
                            'exception_trace_identifiers',
                            [
                                'trace_id' => logs_correlation_trace_id(),
                                'span_id' => \dd_trace_peek_span_id()
                            ]
                        );
                    }

                    $activeSpan = active_span();
                    if (dd_trace_env_config('DD_TRACE_SYMFONY_MESSENGER_DISTRIBUTED_TRACING')
                        && $activeSpan !== $span
                        && $activeSpan === $newTrace) {
                        $integration->setSpanAttributes($activeSpan, 'symfony.messenger.handle_message', 'receive', $envelope, $exception);
                        close_span();

                        if (
                            dd_trace_env_config("DD_TRACE_REMOVE_ROOT_SPAN_SYMFONY_MESSENGER")
                            && dd_trace_env_config("DD_TRACE_REMOVE_AUTOINSTRUMENTATION_ORPHANS")
                        ) {
                            set_distributed_tracing_context("0", "0");
                        }
                    }

                    $integration->setSpanAttributes($span, 'symfony.messenger.handle_message', 'receive', $envelope, $exception);
                },
                'recurse' => true,
            ]
        );

        return self::LOADED;
    }

    private function setSpanAttributes(
        SpanData $span,
        string $name,
        string $operation = null,
        Envelope $envelope = null,
        \Throwable $throwable = null
    ) {
        $span->name = $name;
        $span->service = \ddtrace_config_app_name('symfony');
        $span->type = 'queue';
        $span->meta[Tag::SPAN_KIND] = 'client';
        $span->meta[Tag::COMPONENT] = self::NAME;

        if ($operation !== null && $operation !== '') {
            $span->meta[Tag::MQ_OPERATION] = $operation;
        }

        if ($envelope !== null) {
            $messageName = \get_class($envelope->getMessage());
            $receivedStamp = $envelope->last(ReceivedStamp::class);
            $transportName = $receivedStamp ? $receivedStamp->getTransportName() : null;

            $span->resource = $transportName !== null && $transportName !== ''
                ? \sprintf('%s -> %s', $messageName, $transportName)
                : $messageName;

            $span->meta = \array_merge($span->meta, $this->resolveMetadataFromEnvelope($envelope));
        }

        if ($throwable !== null) {
            $span->exception = $throwable;
        }
    }

    private function resolveMetadataFromEnvelope(Envelope $envelope): array
    {
        $busStamp = $envelope->last(BusNameStamp::class);
        $delayStamp = $envelope->last(DelayStamp::class);
        $receivedStamp = $envelope->last(ReceivedStamp::class);
        $redeliveryStamp = $envelope->last(RedeliveryStamp::class);
        $transportMessageIdStamp = $envelope->last(TransportMessageIdStamp::class);

        $messageName = \get_class($envelope->getMessage());
        $transportName = $receivedStamp ? $receivedStamp->getTransportName() : null;
        $transportMessageId = $transportMessageIdStamp ? $transportMessageIdStamp->getId() : null;

        // AWS SQS
        if (\class_exists(AmazonSqsReceivedStamp::class)) {
            $transportMessageId = $envelope->last(AmazonSqsReceivedStamp::class)?->getId();
        }

        $stamps = [];
        foreach ($envelope->all() as $stampFqcn => $instances) {
            $stamps[$stampFqcn] = \count($instances);
        }

        $metadata = [
            'messaging.symfony.bus' => $busStamp ? $busStamp->getBusName() : null,
            'messaging.symfony.name' => $messageName,
            'messaging.symfony.transport' => $transportName,
            'messaging.symfony.delay' => $delayStamp ? $delayStamp->getDelay() : null,
            'messaging.symfony.retry_count' => $redeliveryStamp ? $redeliveryStamp->getRetryCount() : null,
            'messaging.symfony.redelivered_at' => $redeliveryStamp ? $redeliveryStamp->getRedeliveredAt()->format('Y-m-d\TH:i:sP') : null,
            'messaging.symfony.stamps' => $stamps,
            Tag::MQ_DESTINATION => $transportName,
            Tag::MQ_SYSTEM => 'symfony',
            Tag::MQ_DESTINATION_KIND => 'queue',
            Tag::MQ_MESSAGE_ID => $transportMessageId,
        ];

        return \array_filter($metadata, function ($value): bool {
            if (\is_array($value)) {
                return \count($value) > 0;
            }

            return $value !== null && $value !== '';
        });
    }
}
