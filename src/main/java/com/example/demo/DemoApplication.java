package com.example.demo;

import akka.Done;
import akka.actor.ActorSystem;
import akka.kafka.CommitterSettings;
import akka.kafka.ConsumerMessage;
import akka.kafka.ConsumerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Committer;
import akka.kafka.javadsl.Consumer;
import akka.stream.ActorAttributes;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.Supervision;
import akka.stream.javadsl.Keep;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;

@SpringBootApplication
public class DemoApplication implements InitializingBean, DisposableBean {
	private String[] topics = {"taxilla-events"};
	private String maxPollRecords;
	private String maxPollInterval;
	private String kafkaServers;
	private int maxParallelActor;
	private String msgsToGroup;
	private int  maxSize;
	private Consumer.DrainingControl<Done> streamController;
	private ActorSystem actorSystem;
	private Materializer materializer;
	private ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

	public static void main(String[] args) {
		SpringApplication.run(DemoApplication.class, args);
	}

	public DemoApplication() {
		maxPollRecords = System.getProperty("max.poll.records", "500");
		maxPollInterval = System.getProperty("max.poll.interval", "2147483647");
		msgsToGroup = System.getProperty("max.msgs.group", "20");
		maxSize = Integer.parseInt(System.getProperty("max.size", "1"));
		kafkaServers = System.getProperty("kafka.servers");
		maxParallelActor = Integer.parseInt(System.getProperty("max.parallel.actor", "500"));
		actorSystem = actorSystem();
		materializer = ActorMaterializer.create(actorSystem);
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		pollMessages();
	}

	@Override
	public void destroy() throws Exception {
		System.out.println(Instant.now().toString() + " Application stopped");
	}

	private ActorSystem actorSystem() {
		Properties properties = new Properties();
		//properties.setProperty("akka.loglevel", "INFO");
		//properties.setProperty("akka.stdout-loglevel", "INFO");
		//properties.setProperty("akka.loggers.0", "akka.event.slf4j.Slf4jLogger");
		//properties.setProperty("akka.logging-filter", "akka.event.slf4j.Slf4jLoggingFilter");
		properties.setProperty("akka.actor.provider", "akka.actor.LocalActorRefProvider");
		properties.setProperty("akka.jvm-exit-on-fatal-error", "on");
		return ActorSystem.create("akkaSystem", ConfigFactory.parseProperties(properties));
	}

	public void pollMessages() {
		System.out.println(Instant.now().toString() + " Application starting using properties " + this.toString());

		ConsumerSettings<String, String> consumerSettings = getConsumerSettings(actorSystem);
		CommitterSettings committerSettings = getCommitterSettings(actorSystem);
		streamController = Consumer.committableSource(consumerSettings, Subscriptions.topics(topics))
						.mapAsync(maxParallelActor, msg -> businessLogic(actorSystem, msg)
							.thenApply(done ->  msg.committableOffset()))
						.toMat(Committer.sink(committerSettings), Keep.both())
						.withAttributes(ActorAttributes.supervisionStrategy(ex -> (Supervision.Directive)(Supervision.restart())))
		  				.mapMaterializedValue(Consumer::createDrainingControl)
		  				.run(materializer);
	}

	private CommitterSettings getCommitterSettings(ActorSystem actorSystem) {
		return CommitterSettings.create(actorSystem)
				.withParallelism(maxParallelActor)
				.withMaxBatch(Long.parseLong(msgsToGroup))
				.withMaxInterval(Duration.ofSeconds(30));
	}

	private CompletableFuture<Boolean> businessLogic(ActorSystem actorSystem, ConsumerMessage.CommittableMessage msg) {
		return CompletableFuture.supplyAsync(() -> {
			ConsumerRecord record = msg.record();
			try {
				Thread.sleep(10);
				if(record.offset() % 10 == 0) {
					scheduler.schedule(() ->
									System.out.println(Instant.now().toString() + " delayed processed record " + record.offset() + " partition : " + record.partition()),
							1000, TimeUnit.MILLISECONDS);
				}
				System.out.println(Instant.now().toString() + " processed record " + record.offset() + " partition : " + record.partition());
			} catch(Exception ex) {
				System.out.println(Instant.now().toString() + " failed processing record " + record.offset() + " partition : " + record.partition());
			}
			return true;
		}, actorSystem.dispatcher());
	}

	private ConsumerSettings<String, String> getConsumerSettings(ActorSystem actorSystem) {
		ConsumerSettings<String, String> consumerSettings = ConsumerSettings
				.create(actorSystem, new StringDeserializer(),
						new StringDeserializer())
				// The stage will delay stopping the internal actor to allow processing of messages already in the
				// stream (required for successful committing). This can be set to 0 for streams using DrainingControl
				.withStopTimeout(Duration.ofSeconds(0))
				.withBootstrapServers(kafkaServers)
				.withGroupId("cg-requests-test1")
				.withClientId("eeeeeeeeeeeeee")
				.withProperties(defaultConsumerConfig());
		return consumerSettings;
	}

	public Map<String, String> defaultConsumerConfig(){
		Map<String, String> defaultConsumerConfig = new HashMap<>();
		defaultConsumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		defaultConsumerConfig.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollInterval);
		defaultConsumerConfig.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
		defaultConsumerConfig.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, Integer.toString(maxSize * 1024 * 1024));
		defaultConsumerConfig.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.RoundRobinAssignor");
		return defaultConsumerConfig;
	}

	@Override
	public String toString() {
		return "DemoApplication{" +
				"topics=" + Arrays.toString(topics) +
				", maxPollRecords='" + maxPollRecords + '\'' +
				", maxPollInterval='" + maxPollInterval + '\'' +
				", kafkaServers='" + kafkaServers + '\'' +
				", maxParallelActor=" + maxParallelActor +
				", msgsToGroup='" + msgsToGroup + '\'' +
				", maxSize=" + maxSize +
				'}';
	}
}
