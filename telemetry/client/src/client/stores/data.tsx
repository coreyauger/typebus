const avro = require('avsc');

// data time format "yyyy-MM-dd'T'HH:mm:ss.SSS" Z with tz
export const toDateTime = (date: Date): string => 
    date.getUTCFullYear() + "-" + (date.getUTCMonth()+1) + "-" + date.getUTCDate() + "T" + date.getUTCHours() + ":" + date.getUTCMinutes() + ":" + date.getUTCSeconds() + "." + date.getMilliseconds() + "Z" 


export interface Hb{
    ts: number;
}

export interface EventMeta{
    eventId: string,
    eventType: string,
    source: string,
    correlationId?: string,
    trace: boolean,
    directReply?: string,
    userId?: string,
    socketId?: string,
    responseTo?: string,
    extra: any,
    occurredAt: number
}   

export interface SocketEvent{
    meta: EventMeta,
    payload: any[]
} 

export interface PublishedEvent{
    meta: EventMeta,
    payload: any[]
}

export interface EventType{}

export interface InType extends EventType{
    fqn: string
}
export interface OutType extends EventType{    
    fqn: string
}

export interface ServiceMethod{
    in: InType, 
    out: OutType
}
export interface TypeSchema{
    fqn: string, 
    schema: string
}

export interface ServiceDescriptor{
    service: string,
    serviceId: string,
    upTime: number,
    serviceMethods: ServiceMethod[],
    types: {[id: string]: TypeSchema}
}

export interface Trace{
    service: string;
    serviceId: string;
    event: PublishedEvent
    id?: string;        // CA - this is not part of the schema.. we just append the EventMeta.eventId to this field.
    dir?: string;       // CA - this is not part of the schema.
    time?: number;
}
export interface InEventTrace extends Trace{
    dir: "in"  
}
export interface OutEventTrace extends Trace{
    dir: "out"
}
export interface ExceptionTrace extends Trace{
    dir: "ex"
}

export interface ServiceException{
    message: string;
    stackTrace: string[];
    extra: any
}

export const uuidv4 = () => {
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
      var r = Math.random() * 16 | 0, v = c == 'x' ? r : (r & 0x3 | 0x8);
      return v.toString(16);
    });
  }

// TODO: CA - we want a way to fetch these schema at runtime.
export const Serializers = {    
    hbType: avro.Type.forSchema({"type":"record","name":"Hb","namespace":"io.surfkit.typebus.event","fields":[{"name":"ts","type":"long"}]}),
    publishedEventType: avro.Type.forSchema({"type":"record","name":"PublishedEvent","namespace":"io.surfkit.typebus.event","fields":[{"name":"meta","type":{"type":"record","name":"EventMeta","fields":[{"name":"eventId","type":"string"},{"name":"eventType","type":"string"},{"name":"source","type":"string"},{"name":"correlationId","type":["null","string"]},{"name":"trace","type":"boolean","default":false},{"name":"directReply","type":["null","string"],"default":null},{"name":"userId","type":["null","string"],"default":null},{"name":"socketId","type":["null","string"],"default":null},{"name":"responseTo","type":["null","string"],"default":null},{"name":"extra","type":{"type":"map","values":"string"},"default":{}},{"name":"occurredAt","type":"long","default":0}]}},{"name":"payload","type":"bytes"}]}),
    socketEventType: avro.Type.forSchema({"type":"record","name":"SocketEvent","namespace":"io.surfkit.typebus.event","fields":[{"name":"meta","type":{"type":"record","name":"EventMeta","fields":[{"name":"eventId","type":"string"},{"name":"eventType","type":"string"},{"name":"source","type":"string"},{"name":"correlationId","type":["null","string"]},{"name":"trace","type":"boolean","default":false},{"name":"directReply","type":["null","string"],"default":null},{"name":"userId","type":["null","string"],"default":null},{"name":"socketId","type":["null","string"],"default":null},{"name":"responseTo","type":["null","string"],"default":null},{"name":"extra","type":{"type":"map","values":"string"},"default":{}},{"name":"occurredAt","type":"long","default":0}]}},{"name":"payload","type":"bytes"}]}),
    getServiceDescriptorType: avro.Type.forSchema({"type":"record","name":"GetServiceDescriptor","namespace":"io.surfkit.typebus.event","fields":[{"name":"service","type":"string"}]}),
    serviceDescriptorType: avro.Type.forSchema({"type":"record","name":"ServiceDescriptor","namespace":"io.surfkit.typebus.event","fields":[{"name":"service","type":"string"},{"name":"serviceId","type":"string"},{"name":"upTime","type":{"type":"long","logicalType":"timestamp-millis"}},{"name":"serviceMethods","type":{"type":"array","items":{"type":"record","name":"ServiceMethod","fields":[{"name":"in","type":{"type":"record","name":"InType","fields":[{"name":"fqn","type":"string"}]}},{"name":"out","type":{"type":"record","name":"OutType","fields":[{"name":"fqn","type":"string"}]}}]}}},{"name":"types","type":{"type":"map","values":{"type":"record","name":"TypeSchema","fields":[{"name":"fqn","type":"string"},{"name":"schema","type":"string"}]}}}]}),
    inEventTraceType: avro.Type.forSchema({"type":"record","name":"InEventTrace","namespace":"io.surfkit.typebus.event","fields":[{"name":"service","type":"string"},{"name":"serviceId","type":"string"},{"name":"event","type":{"type":"record","name":"PublishedEvent","fields":[{"name":"meta","type":{"type":"record","name":"EventMeta","fields":[{"name":"eventId","type":"string"},{"name":"eventType","type":"string"},{"name":"source","type":"string"},{"name":"correlationId","type":["null","string"]},{"name":"trace","type":"boolean","default":false},{"name":"directReply","type":["null","string"],"default":null},{"name":"userId","type":["null","string"],"default":null},{"name":"socketId","type":["null","string"],"default":null},{"name":"responseTo","type":["null","string"],"default":null},{"name":"extra","type":{"type":"map","values":"string"},"default":{}},{"name":"occurredAt","type":"long","default":0}]}},{"name":"payload","type":"bytes"}]}}]}),
    outEventTraceType: avro.Type.forSchema({"type":"record","name":"OutEventTrace","namespace":"io.surfkit.typebus.event","fields":[{"name":"service","type":"string"},{"name":"serviceId","type":"string"},{"name":"event","type":{"type":"record","name":"PublishedEvent","fields":[{"name":"meta","type":{"type":"record","name":"EventMeta","fields":[{"name":"eventId","type":"string"},{"name":"eventType","type":"string"},{"name":"source","type":"string"},{"name":"correlationId","type":["null","string"]},{"name":"trace","type":"boolean","default":false},{"name":"directReply","type":["null","string"],"default":null},{"name":"userId","type":["null","string"],"default":null},{"name":"socketId","type":["null","string"],"default":null},{"name":"responseTo","type":["null","string"],"default":null},{"name":"extra","type":{"type":"map","values":"string"},"default":{}},{"name":"occurredAt","type":"long","default":0}]}},{"name":"payload","type":"bytes"}]}}]}),
    exceptionTraceType: avro.Type.forSchema({"type":"record","name":"ExceptionTrace","namespace":"io.surfkit.typebus.event","fields":[{"name":"service","type":"string"},{"name":"serviceId","type":"string"},{"name":"event","type":{"type":"record","name":"PublishedEvent","fields":[{"name":"meta","type":{"type":"record","name":"EventMeta","fields":[{"name":"eventId","type":"string"},{"name":"eventType","type":"string"},{"name":"source","type":"string"},{"name":"correlationId","type":["null","string"]},{"name":"trace","type":"boolean","default":false},{"name":"directReply","type":["null","string"],"default":null},{"name":"userId","type":["null","string"],"default":null},{"name":"socketId","type":["null","string"],"default":null},{"name":"responseTo","type":["null","string"],"default":null},{"name":"extra","type":{"type":"map","values":"string"},"default":{}},{"name":"occurredAt","type":"long","default":0}]}},{"name":"payload","type":"bytes"}]}}]}),    
    serviceExceptionType: avro.Type.forSchema({"type":"record","name":"ServiceException","namespace":"io.surfkit.typebus.event","fields":[{"name":"message","type":"string"},{"name":"stackTrace","type":{"type":"array","items":"string"}},{"name":"extra","type":{"type":"map","values":"string"},"default":{}}]}),    
}

export const SerializerMappings = {
    "io.surfkit.typebus.event.package.Hb": Serializers.hbType,
    "io.surfkit.typebus.event.package.PublishedEvent": Serializers.publishedEventType,
    "io.surfkit.typebus.event.package.SocketEvent": Serializers.socketEventType,
    "io.surfkit.typebus.event.package.GetServiceDescriptor": Serializers.getServiceDescriptorType,
    "io.surfkit.typebus.event.package.ServiceDescriptor": Serializers.serviceDescriptorType,
    "io.surfkit.typebus.event.package.InEventTrace": Serializers.inEventTraceType,
    "io.surfkit.typebus.event.package.OutEventTrace": Serializers.outEventTraceType,
    "io.surfkit.typebus.event.package.ExceptionTrace": Serializers.exceptionTraceType,
    "io.surfkit.typebus.event.package.ServiceException": Serializers.serviceExceptionType,    
}
