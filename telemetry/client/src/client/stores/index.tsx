//import plannerStore from './PlannerStore';
const avro = require('avsc');
import { RouterStore } from 'mobx-react-router';
import { SocketEvent, ServiceDescriptor, Trace, SerializerMappings } from './data'
import { observable, computed } from 'mobx';
import { TypebusSocket, TriggerEvent } from '../socket/WebSocket';
import { RecordType } from 'avro-typescript';

const routingStore = new RouterStore();


class EventStreamStore {
	@observable events: SocketEvent[] = [];
    @observable pendingRequests = 0;

    constructor() {
        //mobx.autorun(() => console.log(this.report));
    }
	addEvent(e: SocketEvent) {
		this.events.push(e);
	}
}

class ServiceStore {
    @observable services: ServiceDescriptor[] = [];
    @observable schema: {[fqn: string]: RecordType} = {};
    constructor(){
        this.schema = SerializerMappings
    }

    addService(s: ServiceDescriptor) {
        this.services.push(s)
        let updates = {}
        Object.keys(s.types).map(x => s.types[x]).forEach( x => {
            console.log("adding FQN: " + x.fqn)
            updates[x.fqn] = avro.Type.forSchema(JSON.parse(x.schema)) as RecordType         
        })
        this.schema = {...this.schema, ...updates}
    }
    removeService(s: ServiceDescriptor){
        this.services = this.services.filter(x => x.serviceId != s.serviceId)
    }
}

class SocketStore{
    typebus: TypebusSocket;
    connect(tb: TypebusSocket){
        this.typebus = tb;
    }
    fireEvent(event: TriggerEvent){
        this.typebus.fireEvent(event);
    }    
}

class TraceStore {
    @observable events: { [id:string]:Trace } = {};
    @computed get stream(){
        return Object.keys(this.events).map(x => this.events[x]).sort( (a, b) => b.time - a.time )
    }
    addTrace(s: Trace) {
        this.events[s.id] = s
    }
}

class ConsumerStore{
    @observable consumers: string[] = [];
    addConsumerId(id: string){
        this.consumers.push(id)
    }
}


class ExternalAccountsStore {
    @observable accounts: { [id:string]:any } = {};    
    addAccount(s: any) {
        console.error("WORD UP !!", s)
        this.accounts[s.accounts.consumerId] = s
    }
}

const eventStreamStore = new EventStreamStore();
const serviceStore = new ServiceStore();
const socketStore = new SocketStore();
const traceStore = new TraceStore();
const externalAccounts = new ExternalAccountsStore();
const consumerStore = new ConsumerStore();

const stores = {
    routing: routingStore,
    eventStreamStore,
    serviceStore,
    socketStore,
    traceStore,
    externalAccounts,
    consumerStore
} 

export default stores;