package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.structures.InclusionDependency;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.*;

public class DependencyWorker extends AbstractBehavior<DependencyWorker.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable {
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ReceptionistListingMessage implements Message {
		private static final long serialVersionUID = -5246338806092216222L;
		Receptionist.Listing listing;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class TaskMessage implements Message, LargeMessageProxy.LargeMessage {
		private static final long serialVersionUID = -4667745204456518160L;
		ActorRef<LargeMessageProxy.Message> dependencyMinerLargeMessageProxy;
		int task;
		DependencyMiner.Task data;
		ActorRef<ResultCollector.Message> resultCollector;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyWorker";

	public static Behavior<Message> create() {
		return Behaviors.setup(DependencyWorker::new);
	}

	private DependencyWorker(ActorContext<Message> context) {
		super(context);

		final ActorRef<Receptionist.Listing> listingResponseAdapter = context.messageAdapter(Receptionist.Listing.class, ReceptionistListingMessage::new);
		context.getSystem().receptionist().tell(Receptionist.subscribe(DependencyMiner.dependencyMinerService, listingResponseAdapter));

		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);
	}

	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(ReceptionistListingMessage.class, this::handle)
				.onMessage(TaskMessage.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(ReceptionistListingMessage message) {
		Set<ActorRef<DependencyMiner.Message>> dependencyMiners = message.getListing().getServiceInstances(DependencyMiner.dependencyMinerService);
		for (ActorRef<DependencyMiner.Message> dependencyMiner : dependencyMiners)
			dependencyMiner.tell(new DependencyMiner.RegistrationMessage(this.getContext().getSelf(), this.largeMessageProxy));
		return this;
	}

	private Behavior<Message> handle(TaskMessage message) {
		DependencyMiner.Task t = message.getData();
		this.getContext().getLog().info("Working! {}:  {} --- {}", message.task, t.getT1().getHeaders(), t.getT2().getHeaders());

		List<DependencyMiner.Result> results = new LinkedList<>();
		ArrayList<Set<String>> t1Entries = new ArrayList<>(t.getT1().getHeaders().length);
		ArrayList<Set<String>> t2Entries = new ArrayList<>(t.getT2().getHeaders().length);

		// t1 entries
		for (int i = 0; i < t.getT1().getHeaders().length; i++) {
			t1Entries.add(new HashSet<>());
		}
		for (String[] entry : t.getT1().getEntries()) {
			for (int i = 0; i < t.getT1().getHeaders().length; i++) {
				t1Entries.get(i).add(entry[i]);
			}
		}

		// t2 entries
		for (int i = 0; i < t.getT2().getHeaders().length; i++) {
			t2Entries.add(new HashSet<>());
		}
		for (String[] entry : t.getT2().getEntries()) {
			for (int i = 0; i < t.getT2().getHeaders().length; i++) {
				t2Entries.get(i).add(entry[i]);
			}
		}

		List<InclusionDependency> idn = new LinkedList<>();

		// check idn between tables
		for (int i = 0; i < t.getT1().getHeaders().length; i++) {
			for (int j = 0; j < t.getT2().getHeaders().length; j++) {
				if (t1Entries.get(i).containsAll(t2Entries.get(j))) {
					getContext().getLog().info("{} contains all {}", t.getT1().getHeaders()[i], t.getT2().getHeaders()[j]);
					InclusionDependency result = new InclusionDependency(
							t.getT1().getFile(),
							new String[]{ t.getT1().getHeaders()[i] },
							t.getT2().getFile(),
							new String[]{ t.getT2().getHeaders()[j] }
					);
					idn.add(result);
				}
				if (t2Entries.get(j).containsAll(t1Entries.get(i))) {
					getContext().getLog().info("{} contains all {}", t.getT2().getHeaders()[j], t.getT1().getHeaders()[i]);
					InclusionDependency result = new InclusionDependency(
							t.getT2().getFile(),
							new String[]{ t.getT2().getHeaders()[j] },
							t.getT1().getFile(),
							new String[]{ t.getT1().getHeaders()[i] }
					);
					idn.add(result);
				}
			}
		}

		// check idn within tables
		if (t.isCheckT1()) {
			checkInternalIdn(t.getT1(), t1Entries, idn);
		}
		if (t.isCheckT2()) {
			checkInternalIdn(t.getT2(), t2Entries, idn);
		}

		getContext().getLog().info("Finished {}! First: {}, Second: {}", message.task, t1Entries.size(), t2Entries.size());
		message.resultCollector.tell(new ResultCollector.ResultMessage(idn));

		LargeMessageProxy.LargeMessage completionMessage = new DependencyMiner.CompletionMessage(this.getContext().getSelf(), largeMessageProxy, results, message.task);
		this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(completionMessage, message.getDependencyMinerLargeMessageProxy()));

		return this;
	}

	private void checkInternalIdn(DependencyMiner.Table t, ArrayList<Set<String>> entries, List<InclusionDependency> idn) {
		for (int i = 0; i < t.getHeaders().length; i++) {
			for (int j = i + 1; j < t.getHeaders().length; j++) {
				checkColumnIdn(t, entries, idn, j, i);
				checkColumnIdn(t, entries, idn, i, j);
			}
		}
	}

	private void checkColumnIdn(DependencyMiner.Table t, ArrayList<Set<String>> entries, List<InclusionDependency> idn, int i, int j) {
		if (entries.get(j).containsAll(entries.get(i))) {
			getContext().getLog().info("{} contains all {}", t.getHeaders()[j], t.getHeaders()[i]);
			InclusionDependency result = new InclusionDependency(
					t.getFile(),
					new String[]{ t.getHeaders()[j] },
					t.getFile(),
					new String[]{ t.getHeaders()[i] }
			);
			idn.add(result);
		}
	}
}
