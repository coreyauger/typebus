# type-bus
Framework for building distributed microserviceies in scala with akka-streams and kafka

As functional programmers we know the value of modeling our system with strong types.  Functions simply are a mapping from one type to another.  Typebus extends this to the service design level.  Typebus defines:
* A **service** is a collection of functions that take a type T => Future[U]
* You can also define a functions that are a sink.  This is a method from T => Future[Unit].  This methods don't show up as part of your service definition

Services need coordination.  Most of the coordination will be around making sure that responses map back to the right request.  This usually requires some state to be stored and managed and for this we use Actors.  Typebus uses actors to emulate RPC calls, in addition to allowing you to create powerful “Ego Centric” abstractions over your services.   I will describe these patterns in more detail below.

* An **Actor** stores state and allows for configurable guarantees for your delivery.
* Orchestration Actors are one way of performing composition. This is where transaction logic could be placed for a particular user, routing logic, and fallback behaviours.

The goal of typebus is then to abstract away the communication concerns and allow the programer to focus on the logic expressed in types.  This will be achieved by providing support for multiple bus layers “under the hood”, kafka, kinesis, and possibly alternatives could have support written in at this layer.

Another goal of typebus is to provide a broadcast of the available of service functionality.  This in combination with an avro schema repository can be used for rapid prototyping in dynamic languages.  The logic can be tested, and when proven transferred to actor based compositions at scale in production.

Typebus services maintain a service definition.  This definitions contain the mappings and type information and can be fetched from other services over the bus.  This allows programmers to generate RPC clients and types without writing or knowing anything other then the service name.

## Typebus Design Patterns

**Transformation Pattern**
This is the simplest and probably most widely used pattern in the system.  With this pattern we are creating a service that takes in a type T and at some point in the future will produce a type U or an Error.  When one event does not need to know about others we can simple create causal chains of events.

It is worth mentioning that this pattern is fully horizontally scalable and fault tolerant.  Each additional node will provide extra compute cycles in addition to being a redundant point against failure.

**Cluster Pattern**
One additional need that occurs when we are scaling a large system is how to coordinate state.  Here is where akka clustering fits in.  multiple options on how you would perform RPC.  
1) You could again use the generated client inside the actor.  This is one way to achieve RPC.
2) You could setup a Type sink for the expected return type in your service and when the service receives it forward it again to the actor. 
                                                                                                                                        
Clustering should be the goto option when you need to manage some state or lifecycle about events.  For example you can achieve idempotent requests using this model.
 
