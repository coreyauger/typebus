import * as React from 'react';
import {observer} from 'mobx-react';
import { ServiceDescriptor, ServiceMethod, Trace, PublishedEvent, ExceptionTrace, OutEventTrace, InEventTrace } from '../stores/data'
import { Theme } from '@material-ui/core/styles/createMuiTheme';
import createStyles from '@material-ui/core/styles/createStyles';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import Grid from '@material-ui/core/Grid';
import ServiceCard from '../components/ServiceCard'
import TriggerEventDialog from '../components/TriggerEventDialog'
import { TriggerEvent } from '../socket/WebSocket';
import withRoot from '../withRoot';
import TraceTable from '../components/TraceTable';
import TraceEventDetails from '../components/TraceEventDetails';
import { Dialog } from '@material-ui/core';

const styles = (theme: Theme) =>
  createStyles({
    root: {
      
    },
    tableContainer: {
      height: 320,
    },
    paper: {
      padding: theme.spacing.unit * 2,
      textAlign: 'center',
      color: theme.palette.text.secondary,
    },
  });

type State = {
  selectedMethod: ServiceMethod;  
  selectedTrace: InEventTrace | OutEventTrace | ExceptionTrace
};

interface Props {
  store: any;  
  service?: string
};

@observer
class Services extends React.Component<Props & WithStyles<typeof styles>, State> {
  state = {
    selectedMethod: undefined,
    selectedTrace: undefined   
  };
 
  onMethodSelect =  (method: ServiceMethod) => {
    this.setState({...this.state, selectedMethod: method});
  }
  clearMethodSelect = () => {
    this.setState({...this.state, selectedMethod: undefined});
  }
  onServiceTypeSelect = (serviceDescriptor: ServiceDescriptor) => {
    const { location, push, goBack } = this.props.store.routing; // CA - inject above did not work.. you should see this as a "prop" (investigate)
    push('/p/service/'+serviceDescriptor.service)
  }
  onTriggerEvent = (event: TriggerEvent) => {
    this.props.store.socketStore.fireEvent(event)
  }
  handleTraceClose = () =>{
    this.setState({...this.state, selectedTrace: undefined})
  }
  onTraceSelect = (trace: InEventTrace | OutEventTrace | ExceptionTrace) =>{
    this.setState({...this.state, selectedTrace: trace})
  }

  render() {    
    const { location, push, goBack } = this.props.store.routing; // CA - inject above did not work.. you should see this as a "prop" (investigate)
    const traceStream = this.props.store.traceStore.events
    const pathname = location.pathname.split('/')
    const selectedService = this.props.service ? this.props.service : (pathname.length == 4) ? pathname[3] : undefined;
    console.log("service", this.props.service)
    
    const serviceList = this.props.store.serviceStore.services
    const selectServiceDescriptor = this.props.store.serviceStore.services.find(x => x.service == selectedService)
    console.log(selectServiceDescriptor)
    const  classes = this.props.classes;
    return (<div className={this.props.classes.root}>
      {selectServiceDescriptor ? (
         <Grid container spacing={24}> 
            <Grid item xs={4} key={selectServiceDescriptor.serviceId}><ServiceCard serviceDescriptor={selectServiceDescriptor} onMethodSelect={this.onMethodSelect} expanded={true} /></Grid>
            <Grid item xs={8} key="traceStream">
              <TraceTable store={this.props.store} service={selectServiceDescriptor.service} onSelect={this.onTraceSelect} />
            </Grid>
         </Grid>
      ):(
      <Grid container spacing={24}>
          {serviceList.map( (x: ServiceDescriptor) => <Grid item xs={4} key={x.serviceId}><ServiceCard serviceDescriptor={x} onMethodSelect={this.onMethodSelect} onServiceSelect={this.onServiceTypeSelect} /></Grid> )}
      </Grid>)}
      <TriggerEventDialog open={this.state.selectedMethod != undefined} method={this.state.selectedMethod} store={this.props.store} onClose={this.clearMethodSelect} onTriggerEvent={this.onTriggerEvent} />  
      <Dialog onClose={this.handleTraceClose} aria-labelledby="simple-dialog-title" open={this.state.selectedTrace != undefined} maxWidth="lg">
        <TraceEventDetails trace={this.state.selectedTrace} store={this.props.store} />
      </Dialog>
      </div>);       
  }
}

export default withRoot(withStyles(styles)(Services));