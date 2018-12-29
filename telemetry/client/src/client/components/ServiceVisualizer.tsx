import * as React from 'react';
import {createRef} from 'react';
import {observer} from 'mobx-react';
import { Theme } from '@material-ui/core/styles/createMuiTheme';
import createStyles from '@material-ui/core/styles/createStyles';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import withRoot from '../withRoot';
import D3Graph from './svg/D3Graph';

import { ServiceDescriptor, Trace } from '../stores/data';

const styles = (theme: Theme) =>
  createStyles({
    root: {
      textAlign: 'center'      
    },
  });

export interface Props{
    store: any,
    selected: Trace[]
}

type State = {
    clientWidth: number;
    clientHeight: number;
}

@observer
class ServiceVisualizer extends React.Component<Props & WithStyles<typeof styles>, State> {
  containerRef = createRef<HTMLDivElement>();
  state = {
    clientWidth: 1000,
    clientHeight: 450
  }
  serviceEventId = (x: Trace) => x.event.meta.eventId + "-" + x.service +"-"+x.dir
  shortEventName = (x: Trace) => x.event.meta.eventType.split(".")[x.event.meta.eventType.split(".").length-1]

  buildGraph = () =>{
    const kinesis = {id: "Kinesis", group: 0, radius: 15}
    let serviceGroupInc = 1
    let serviceGroupMap = {}
    const serviceList = this.props.store.serviceStore.services.map( (x: ServiceDescriptor) => {
        if(serviceGroupMap[x.service])return null;
        serviceGroupMap[x.service] = serviceGroupInc++;
        return {
            id: x.service,
            group: serviceGroupMap[x.service],
            name: x.service,
            radius: 5.0
        };
    }).filter(x => x)
    const traceStream = this.props.store.traceStore.stream.map( (x: Trace) => {
        return [{
            id: this.serviceEventId(x),
            group: serviceGroupMap[x.service],            
            time: x.time,
            dir: x.dir,
            name: this.shortEventName(x),
            radius: 2
        },
        {
            id: "K-"+this.serviceEventId(x),
            group: 0,
            time: x.time,
            dir: x.dir,
            name: this.shortEventName(x),
            radius: 2
        }];
    }).reduce( (a, b) => [...a, ...b], [] )
    const traceStreamLinks = this.props.store.traceStore.stream.map( (x: Trace) => {
        const isSelected = this.props.selected.indexOf(x) >= 0
        return [
            {source: "Kinesis", target: "K-"+this.serviceEventId(x), value: 0.5, isSelected},
            {source: "K-"+this.serviceEventId(x), target: this.serviceEventId(x), value: 1, isSelected},
            {source: this.serviceEventId(x), target: x.service, value: 0.5, isSelected}];
    }).reduce( (a, b) => [...a, ...b], [] )

    return {
        nodes: [
          kinesis,
          ...serviceList,
          ...traceStream
        ],
        links: [
            ...traceStreamLinks
        ]
    }
  }
  componentDidMount() {
    console.log("ref", this.containerRef.current)
    const clientWidth = this.containerRef.current.clientWidth
    const clientHeight = this.containerRef.current.clientHeight
    this.setState({...this.state, clientWidth, clientHeight})
  } 

  render() {
    const graph = this.buildGraph();       
    return (         
      <div className={this.props.classes.root} ref={this.containerRef}>      
        <D3Graph 
            /*key={"graph"+graph.nodes.length} */
            width={this.state.clientWidth} 
            height={this.state.clientHeight} 
            graph={graph} /> 
      </div>
    );
  }
}

export default withRoot(withStyles(styles)(ServiceVisualizer));
