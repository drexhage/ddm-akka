package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.Terminated;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.InputConfigurationSingleton;
import de.ddm.singletons.SystemConfigurationSingleton;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.File;
import java.util.*;

public class DependencyMiner extends AbstractBehavior<DependencyMiner.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage {
	}

	@NoArgsConstructor
	public static class StartMessage implements Message {
		private static final long serialVersionUID = -1963913294517850454L;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ContentMessage implements Message {
		private static final long serialVersionUID = -5322425954432915838L;
		int id;
		File file;
		String[] header;
		List<String[]> content;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class RegistrationMessage implements Message {
		private static final long serialVersionUID = -4025238529984914107L;
		ActorRef<DependencyWorker.Message> dependencyWorker;
		ActorRef<LargeMessageProxy.Message> proxy;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class CompletionMessage implements Message {
		private static final long serialVersionUID = -7642425159675583598L;
		ActorRef<DependencyWorker.Message> dependencyWorker;
		ActorRef<LargeMessageProxy.Message> proxy;
		List<Result> results;
		Integer task;
	}

	////////////////////////
	// Helper Classes     //
	////////////////////////

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class Result {
		private String header1;
		private String header2;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class Table {
		private File file;
		private String[] headers;
		private List<String[]> entries;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class Task {
		private Table t1;
		private Table t2;
		private boolean checkT1;
		private boolean checkT2;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyMiner";

	public static final ServiceKey<DependencyMiner.Message> dependencyMinerService = ServiceKey.create(DependencyMiner.Message.class, DEFAULT_NAME + "Service");

	public static Behavior<Message> create() {
		return Behaviors.setup(DependencyMiner::new);
	}

	private DependencyMiner(ActorContext<Message> context) {
		super(context);
		this.discoverNaryDependencies = SystemConfigurationSingleton.get().isHardMode();
		this.inputFiles = InputConfigurationSingleton.get().getInputFiles();
		this.headerLines = new String[this.inputFiles.length][];

		this.inputReaders = new ArrayList<>(inputFiles.length);
		for (int id = 0; id < this.inputFiles.length; id++)
			this.inputReaders.add(context.spawn(InputReader.create(id, this.inputFiles[id]), InputReader.DEFAULT_NAME + "_" + id));
		this.resultCollector = context.spawn(ResultCollector.create(), ResultCollector.DEFAULT_NAME);
		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);

		this.dependencyWorkers = new LinkedList<>();

		context.getSystem().receptionist().tell(Receptionist.register(dependencyMinerService, context.getSelf()));
	}

	/////////////////
	// Actor State //
	/////////////////

	private long startTime;

	private final boolean discoverNaryDependencies;
	private final File[] inputFiles;
	private final String[][] headerLines;

	private final List<ActorRef<InputReader.Message>> inputReaders;
	private final ActorRef<ResultCollector.Message> resultCollector;
	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;
	private final Queue<RegistrationMessage> dependencyWorkers;
	private Queue<Task> queue = new LinkedList<>();
	private final List<Table> tables = new LinkedList<>();
	private final HashMap<File, Boolean> internalIdn = new HashMap<>();
	private final Map<Integer, Task> pending = new HashMap<>();
	private int taskCount = 0;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(StartMessage.class, this::handle)
				.onMessage(ContentMessage.class, this::handle)
				.onMessage(RegistrationMessage.class, this::handle)
				.onMessage(CompletionMessage.class, this::handle)
				.onSignal(Terminated.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(StartMessage message) {
		for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
			inputReader.tell(new InputReader.ReadAllMessage(this.getContext().getSelf()));
		this.startTime = System.currentTimeMillis();
		this.getContext().getLog().info("Starting");
		return this;
	}

	private Behavior<Message> handle(ContentMessage message) {
		Table newTable = new Table(message.file, message.header, message.content);
		for (Table otherTable : tables) {
			boolean checkT1 = internalIdn.get(newTable.file) == null;
			boolean checkT2 = internalIdn.get(otherTable.file) == null;
			queue.add(new Task(newTable, otherTable, checkT1, checkT2));
			internalIdn.put(newTable.file, true);
			internalIdn.put(otherTable.file, true);
		}
		tables.add(newTable);
		handleFreeWorker();
		return this;
	}

	private Behavior<Message> handle(RegistrationMessage message) {
		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		if (!this.dependencyWorkers.contains(message)) {
			this.getContext().watch(dependencyWorker);
			dependencyWorkers.add(message);
		}
		handleFreeWorker();
		return this;
	}

	private Behavior<Message> handle(CompletionMessage message) {
		this.getContext().getLog().info("Finished {}", message.task);
		pending.remove(message.task);

		dependencyWorkers.add(new RegistrationMessage(message.getDependencyWorker(), message.getProxy()));
		handleFreeWorker();

		// At some point, I am done with the discovery. That is when I should call my end method. Because I do not work on a completable task yet, I simply call it after some time.
		if (pending.isEmpty() && queue.isEmpty()) {
			getContext().getLog().info("Ending");
			this.end();
		}
		return this;
	}

	private void handleFreeWorker() {
		while (!queue.isEmpty() && !dependencyWorkers.isEmpty()) {
			RegistrationMessage register  = dependencyWorkers.poll();
			Task taskData = queue.poll();
			taskCount++;
			this.getContext().getLog().info("Starting task {} ({} - {})", taskCount, taskData.t1.headers, taskData.t2.headers);
			DependencyWorker.TaskMessage taskMessage = new DependencyWorker.TaskMessage(this.largeMessageProxy, taskCount, taskData, this.resultCollector);
			LargeMessageProxy.SendMessage largeMessage = new LargeMessageProxy.SendMessage(taskMessage, register.getProxy());
			pending.put(taskCount, taskData);
			largeMessageProxy.tell(largeMessage);
		}
	}

	private void end() {
		this.resultCollector.tell(new ResultCollector.FinalizeMessage());
		long discoveryTime = System.currentTimeMillis() - this.startTime;
		this.getContext().getLog().info("Finished mining within {} ms!", discoveryTime);
	}

	private Behavior<Message> handle(Terminated signal) {
		ActorRef<DependencyWorker.Message> dependencyWorker = signal.getRef().unsafeUpcast();
		getContext().getLog().info("Terminated");
		this.dependencyWorkers.remove(dependencyWorker);
		return this;
	}
}