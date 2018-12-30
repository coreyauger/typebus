import {ServiceDescriptor, SocketEvent, EventMeta, toDateTime, Serializers, InEventTrace, ExceptionTrace, OutEventTrace, SerializerMappings} from '../stores/data';
import stores from '../stores'
import { avroToTypeScript, RecordType } from "avro-typescript"
const avro = require('avsc');

const WS_OPEN = 1

const typeMap = {
    "io.surfkit.typebus.event.package.ServiceDescriptor" : (socketEvent: SocketEvent) =>{        
        const data = Serializers.serviceDescriptorType.fromBuffer( Buffer.from(socketEvent.payload) )
        console.log("data2", data)
        const serviceDescriptor = data as ServiceDescriptor
        console.log("serviceDescriptor", serviceDescriptor)
        stores.serviceStore.addService(serviceDescriptor)
        // CA - you can gen typescript types below 
        /*
        serviceDescriptor.serviceMethods.map(x => {
            const schemaIn = JSON.parse(x.in.schema) as RecordType;
            console.log(avroToTypeScript(schemaIn as RecordType));
            const schemaOut = JSON.parse(x.out.schema) as RecordType;
            console.log(avroToTypeScript(schemaOut as RecordType));
        })*/   
    },
    "io.surfkit.typebus.event.package.InEventTrace" : (socketEvent: SocketEvent) =>{        
        const data = Serializers.inEventTraceType.fromBuffer( Buffer.from(socketEvent.payload) )
        console.log("data", data)
        const trace = data as InEventTrace
        trace.id = socketEvent.meta.eventId
        trace.dir = "in"
        trace.time = socketEvent.meta.occurredAt
        stores.traceStore.addTrace(trace)  
    },
    "io.surfkit.typebus.event.package.OutEventTrace" : (socketEvent: SocketEvent) =>{        
        const data = Serializers.outEventTraceType.fromBuffer( Buffer.from(socketEvent.payload) )
        console.log("data", data)
        const trace = data as OutEventTrace
        trace.id = socketEvent.meta.eventId
        trace.dir = "out"
        trace.time = socketEvent.meta.occurredAt
        stores.traceStore.addTrace(trace)  
    },
    "io.surfkit.typebus.event.package.ExceptionTrace" : (socketEvent: SocketEvent) =>{        
        const data = Serializers.exceptionTraceType.fromBuffer( Buffer.from(socketEvent.payload) )
        console.log("data", data)
        const trace = data as ExceptionTrace
        trace.id = socketEvent.meta.eventId 
        trace.dir = "ex"
        trace.time = socketEvent.meta.occurredAt
        stores.traceStore.addTrace(trace)  
    },      
}
 

const toArrayBuffer = (blob: Blob): Promise<Uint8Array> => {
    return new Promise(resolve => {
        let arrayBuffer;
        const fileReader = new FileReader();
        fileReader.onload = async function(event: any) {        
            arrayBuffer = event.target.result;
            resolve(new Uint8Array(arrayBuffer));
        };
        fileReader.readAsArrayBuffer(blob);
    });
}

export interface TriggerEvent{
    event: object,
    fqn: string,
    avroSchema: RecordType,
    delay?: number
}

export class TypebusSocket{

    ws: WebSocket = null
    userId = "bc5e5dd1-fc80-4cfd-9a26-b8e60b43b8b2"
    cancelable: any = null
    queue = []
    
    constructor(uuid: string){
        this.userId = uuid
        this.ws = new WebSocket("ws://localhost:8181/v1/ws/" + this.userId)
        this.ws.onopen =  (event) => {
            console.log("WEBSOCKET IS CONNECTED3 !!!")
            //ws.send("Here's some text that the server is urgently awaiting!"); 

            const socketEvent = this.mkSocketEvent("io.surfkit.typebus.event.package.GetServiceDescriptor", Serializers.getServiceDescriptorType.toBuffer({
                service: ""
            }))
            const buf = Serializers.socketEventType.toBuffer(socketEvent)
            console.log("Getting service descriptors")
            this.ws.send(Serializers.socketEventType.toBuffer(socketEvent))
            while(this.queue.length != 0){
                const data = this.queue.shift()
                this.send(data.eventType, data.payload)
            }
        }
        this.ws.onmessage = async (event) => {
            console.log("got a WS message", event)
            const buffer = await toArrayBuffer(event.data)
            const socketEvent = Serializers.socketEventType.fromBuffer( Buffer.from(buffer) ) as SocketEvent
            stores.eventStreamStore.addEvent(socketEvent)
            console.log("ws socketEvent["+socketEvent.meta.eventType+"]", socketEvent)
            if(typeMap[socketEvent.meta.eventType]){
                typeMap[socketEvent.meta.eventType](socketEvent)
            }                    
        }   
        this.ws.onclose = (event) => {
            console.error("WebSocket is closed now. !!!!!!!!!!!!!!!!!!!");
            if(this.cancelable){
                clearInterval(this.cancelable);
                this.cancelable = null;
            }
        };   
        this.cancelable = setInterval(() =>{
            const hbSocketEvent = this.mkSocketEvent("io.surfkit.typebus.event.package.Hb", Serializers.hbType.toBuffer({
                ts: new Date().getTime()
            }))
            console.log("lub dub..")
            this.ws.send(Serializers.socketEventType.toBuffer(hbSocketEvent))
        }, 25 * 1000) // every 25 seconds send a heart-beat to keep alive
    }

    fireEvent(event: TriggerEvent){
        const e = event;
        setTimeout(() =>{
            const socketEvent = this.mkSocketEvent(e.fqn, avro.Type.forSchema(e.avroSchema).toBuffer(e.event))
            console.log("TRIGGERING EVENT: " + e.fqn, e)
            this.ws.send(Serializers.socketEventType.toBuffer(socketEvent))
        }, (e.delay || 0) * 1000 )
    }

    send(eventType: string, payload: any){
        if(this.ws.readyState != WS_OPEN){
            this.queue.push({eventType, payload})
        }else{
            const socketEvent = this.mkSocketEvent(eventType, payload)
            console.log("SEND EVENT: " + eventType)
            this.ws.send(Serializers.socketEventType.toBuffer(socketEvent))
        }
    }

    mkSocketEvent = (eventType: string, payload: any) => (
        {
            meta: {
                eventId: "",
                eventType: eventType,
                source: "",
                correlationId: new Date().getTime() + "",
                trace: false,
                directReply: null,            
                userId: this.userId,
                socketId: null, 
                responseTo: null,
                extra: {},
                occurredAt: new Date().getTime()
            } as EventMeta,
            payload: payload
        }
    )
}






