package com.primer.reactiverabbitmq;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.annotation.PostConstruct;
import java.time.Duration;

@RestController
public class AmqpReactiveController {
    @Autowired
    private AmqpAdmin amqpAdmin;

    @Autowired
    private DestinationsConfig destinationsConfig;

    @Autowired
    private AmqpTemplate template;

    @Autowired
    private MessageListenerContainerFactory messageListenerContainerFactory;

    @PostConstruct
    public void setupQueueDestinations() {
        destinationsConfig.getQueues()
                .forEach((key, destination) -> {
                    Exchange exchange = ExchangeBuilder.directExchange(destination.getExchange())
                            .durable(true)
                            .build();
                    amqpAdmin.declareExchange(exchange);
                    Queue queue = QueueBuilder.durable(destination.getRoutingKey()).build();
                    amqpAdmin.declareQueue(queue);
                    Binding binding = BindingBuilder.bind(queue)
                            .to(exchange)
                            .with(destination.getRoutingKey())
                            .noargs();
                    amqpAdmin.declareBinding(binding);
                });
    }

    @PostMapping("/queue/{name}")
    public Mono<ResponseEntity<?>> sendMessageToQueue(@PathVariable String name,
                                                      @RequestBody String playload) {
        final DestinationsConfig.DestinationInfo destinationInfo = destinationsConfig.getQueues().get(name);

        if (destinationInfo == null) {
            return Mono.just(ResponseEntity.notFound().build());
        }
        return Mono.fromCallable(() -> {
            template.convertAndSend(
                    destinationInfo.getExchange(),
                    destinationInfo.getRoutingKey(),
                    playload
            );
            return ResponseEntity.accepted().build();
        });
    }

    @GetMapping(value = "/queue/{name}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<?> receiveMessagesFromQueue(@PathVariable String name) {
        DestinationsConfig.DestinationInfo destinationInfo = destinationsConfig.getQueues().get(name);

        if (destinationInfo == null) {
            return Flux.just(ResponseEntity.notFound().build());
        }

        MessageListenerContainer mlc = messageListenerContainerFactory.createMessageListenerContainer(destinationInfo.getRoutingKey());
        Flux<String> flux = Flux.<String>create(emmiter -> {
            mlc.setupMessageListener((MessageListener)message -> {
                String playload = new String(message.getBody());
                emmiter.next(playload);
            });
            emmiter.onRequest(v -> {mlc.start();});
            emmiter.onDispose(() -> {mlc.stop();});
        });
        return Flux.interval(Duration.ofSeconds(5))
                .map(v -> "SUCCESS")
                .mergeWith(flux);
    }

    @PostConstruct
    public void setupTopicDestinations() {
        destinationsConfig.getTopics()
                .forEach((key, destination) -> {
                    Exchange exchange = ExchangeBuilder.topicExchange(destination.getExchange())
                            .durable(true)
                            .build();
                    amqpAdmin.declareExchange(exchange);
                });
    }

    @PostMapping(value = "/topic/{name}")
    public Mono<ResponseEntity<?>> sendMessageToTopic(@PathVariable String name, @RequestBody String playLoad) {
        DestinationsConfig.DestinationInfo destinationInfo = destinationsConfig.getTopics().get(name);
        if (destinationInfo == null) {
            return Mono.just(ResponseEntity.notFound().build());
        }

        return Mono.fromCallable(() -> {
            template.convertAndSend(destinationInfo.getExchange(), destinationInfo.getRoutingKey(), playLoad);
            return ResponseEntity.accepted().build();
        });
    }

    @GetMapping(value = "/topic/{name}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<?> receiveMessagesFromTopic(@PathVariable String name) {
        DestinationsConfig.DestinationInfo destinationInfo = destinationsConfig.getQueues().get(name);
        if (destinationInfo == null) {
            return Flux.just(ResponseEntity.notFound().build());
        }
        Queue topicQueue = createTopicQueue(destinationInfo);
        String qname = topicQueue.getName();
        MessageListenerContainer mlc = messageListenerContainerFactory.createMessageListenerContainer(qname);
        Flux<String> flux = Flux.<String>create(emmiter -> {
            mlc.setupMessageListener((MessageListener)message -> {
                String payLoad = new String(message.getBody());
                emmiter.next(payLoad);
            });
            emmiter.onRequest(v -> mlc.start());
            emmiter.onDispose(() -> {
                amqpAdmin.deleteQueue(qname);
                mlc.stop();
            });
        });
        return Flux.interval(Duration.ofSeconds(5))
                .map(v -> "SUCCESS!!!")
                .mergeWith(flux);
    }

    private Queue createTopicQueue(DestinationsConfig.DestinationInfo destinationInfo) {
        Exchange exchange = ExchangeBuilder.topicExchange(destinationInfo.getExchange())
                .durable(true)
                .build();
        amqpAdmin.declareExchange(exchange);
        Queue queue = QueueBuilder.nonDurable().build();
        amqpAdmin.declareQueue(queue);
        Binding binding = BindingBuilder
                .bind(queue)
                .to(exchange)
                .with(destinationInfo.getRoutingKey())
                .noargs();
        return queue;
    }
}
