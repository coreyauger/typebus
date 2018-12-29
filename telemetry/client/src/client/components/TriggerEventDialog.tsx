import * as React from 'react';
import AceEditor from 'react-ace';
import { ServiceMethod } from '../stores/data'
import { Theme } from '@material-ui/core/styles/createMuiTheme';
import createStyles from '@material-ui/core/styles/createStyles';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import Tabs from '@material-ui/core/Tabs';
import Tab from '@material-ui/core/Tab';
import AppBar from '@material-ui/core/AppBar';
import Button from '@material-ui/core/Button';
import DialogTitle from '@material-ui/core/DialogTitle';
import Dialog from '@material-ui/core/Dialog';
import InputAdornment from '@material-ui/core/InputAdornment';
import TextField from '@material-ui/core/TextField';
import AlarmAdd from '@material-ui/icons/AlarmAdd';
import withRoot from '../withRoot';
import { RecordType } from 'avro-typescript';
import { TriggerEvent } from '../socket/WebSocket';
import * as brace from 'brace';

import 'brace/mode/javascript';
import 'brace/theme/github';

export interface Schema {}

export type Type = NameOrType | NameOrType[];
export type NameOrType = TypeNames | RecordType | ArrayType | NamedType;
export type TypeNames = "record" | "array" | "null" | "map" | string;

export interface Field {
	name: string;
	type: Type;
	default?: string | number | null | boolean;
}

export interface BaseType {
	type: TypeNames;
}

export interface RecordType extends BaseType {
	type: "record";
	name: string;
	fields: Field[];
}

export interface ArrayType extends BaseType {
	type: "array";
	items: Type;
}

export interface MapType extends BaseType {
	type: "map";
	values: Type;
}

export interface EnumType extends BaseType {
	type: "enum";
	name: string;
	symbols: string[];
}

export interface NamedType extends BaseType {
	type: string;
}

export function isRecordType(type: BaseType): type is RecordType {
	return type.type === "record";
}

export function isArrayType(type: BaseType): type is ArrayType {
	return type.type === "array";
}

export function isMapType(type: BaseType): type is MapType {
	return type.type === "map";
}

export function isEnumType(type: BaseType): type is EnumType {
	return type.type === "enum";
}

export function isUnion(type: Type): type is NamedType[] {
	return type instanceof Array;
}

export function isOptional(type: Type): boolean {
	if (isUnion(type)) {
		const t1 = type[0];
		if (typeof t1 === "string") {
			return t1 === "null";
		}
	}
}


const styles = (theme: Theme) =>
  createStyles({
    root: {
      
    },   
    button: {
      margin: theme.spacing.unit,
    },
    buttonBar: {
      textAlign: "right"
    },
    editorContainer:{
      paddingTop: "20px"
    }
  });

interface State {
  tab: number;
  delay: number;
}

interface Props {
  onClose: () => void;  
  method: ServiceMethod;  
  open: boolean
  onTriggerEvent: (TriggerEvent) => void;
};

class TriggerEventDialog extends React.Component<Props & WithStyles<typeof styles>, State> {
  state = {
    tab: 0,
    delay: 2
  }
  json: ""

  handleClose = () => {
    this.props.onClose();
  };
  handleTabChange = (event, tab) => {
    this.setState({...this.state, tab });
  };
  onChange = (newValue) => {
    this.json = newValue;
  }
  onTriggerEvent = () => {
    console.log("this.state.delay",this.state.delay)
    console.log("triggerEvent", this.json)
    this.props.onTriggerEvent({
      event: JSON.parse(this.json), 
      fqn: this.props.method.in.fqn,
      avroSchema: JSON.parse(this.props.method.in.schema) as RecordType,
      delay: this.state.delay} as TriggerEvent)
    this.handleClose();
  }

  convertFieldDec(field: Field, buffer: string): string {
    // Union Type
    return `\t"${field.name}": ${this.convertType(field.type, buffer)};`;
  }

  convertEnum(enumType: EnumType, buffer: string): string {
    const enumDef = `export enum ${enumType.name} { ${enumType.symbols.join(", ")} };\n`;
    buffer += enumDef;
    return enumType.name;
  }

  convertPrimitive(avroType: string): string {
    switch (avroType) {
      case "long":
      case "int":
      case "double":
      case "float":
        return "number";
      case "bytes":
        return "Buffer";
      case "null":
        return "null | undefined";
      case "boolean":
        return "boolean";
      default:
        return null;
    }
  }

  convertType(type: Type, buffer: string): string {
    console.log("convertType", type)
    // if it's just a name, then use that
    if (typeof type === "string") {
      return this.convertPrimitive(type) || type;
    } else if (type instanceof Array) {
      // array means a Union. Use the names and call recursively
      return type.map(t => this.convertType(t, buffer)).join(" | ");
    } else if (isRecordType(type)) {
      //} type)) {
      // record, use the name and add to the buffer
      return this.convertRecord(type, buffer);
    } else if (isArrayType(type)) {
      // array, call recursively for the array element type
      return this.convertType(type.items, buffer) + "[]";
    } else if (isMapType(type)) {
      // Dictionary of types, string as key
      return `{ [index:string]:${this.convertType(type.values, buffer)} }`;
    } else if (isEnumType(type)) {
      // array, call recursively for the array element type
      return this.convertEnum(type, buffer);
    } else {
      console.error("Cannot work out type", type);
      return "UNKNOWN";
    }
  }

  convertRecord(recordType: RecordType, buffer: string): string {
    buffer += `{\n`;        
    for (let field of recordType.fields) {
      console.log("field",field)
      buffer += this.convertFieldDec(field, buffer) + "\n";
      console.log("buffer: " + buffer)
    }
    buffer += "}\n";
    return buffer;
  }

  avroToJson = (avro: RecordType): string => {
    let buffer = ``;
    console.log("Convering AVRO to Json", avro)
    buffer += this.convertRecord(avro, buffer);
    console.log("bufferEND: "+buffer)
    return buffer;
  }
  onDelayChange = (event) =>{
    this.setState({...this.state, delay: parseFloat(event.target.value) })
  }

  render() {    
    const method = this.props.method;
    const classes = this.props.classes;
    return (
      <Dialog onClose={this.handleClose} aria-labelledby="simple-dialog-title" open={this.props.open} maxWidth="lg">
        <DialogTitle id="te-dialog-title">{method ? method.in.fqn : ""}</DialogTitle>

        <AppBar position="static" color="default">
          <Tabs
            value={this.state.tab}
            onChange={this.handleTabChange}
            indicatorColor="primary"
            textColor="primary"
            fullWidth
          >
            <Tab label="json event" />
            <Tab label="avro schema" />            
          </Tabs>
        </AppBar>
        <div className={classes.editorContainer}>
          <AceEditor
            height="700px"
            width="900px"
            mode="javascript"
            theme="github"            
            onChange={this.onChange}
            setOptions={ {readOnly: this.state.tab == 1} }
            name={method && method.in.schema}
            editorProps={{$blockScrolling: true}}
            value={method && (this.state.tab == 0 ? this.avroToJson(JSON.parse(method.in.schema) as RecordType) : JSON.stringify(JSON.parse(method.in.schema),null,'\t') )}
          />     
          <div className={classes.buttonBar}>
          <TextField
            label="delay seconds"
            type="number"
            onChange={this.onDelayChange}
            value={this.state.delay + ""}
            InputProps={{
              startAdornment: (
                <InputAdornment position="start">
                  <AlarmAdd />
                </InputAdornment>
              ),
            }}
          />

          <Button onClick={this.handleClose} className={classes.button}>Cancel</Button>
            <Button variant="contained" color="primary" disabled={this.json == ""} className={classes.button} onClick={this.onTriggerEvent} >Trigger</Button>           
          </div>
        </div>
      </Dialog>
    ); 
  }
}

export default withRoot(withStyles(styles)(TriggerEventDialog));