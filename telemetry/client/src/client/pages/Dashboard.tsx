import * as React from 'react';
import {observer} from 'mobx-react';
import { InEventTrace, OutEventTrace, ExceptionTrace, Trace } from '../stores/data'
import ServiceVisualizer from '../components/ServiceVisualizer'
import { Theme } from '@material-ui/core/styles/createMuiTheme';
import createStyles from '@material-ui/core/styles/createStyles';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import Paper from '@material-ui/core/Paper';
import Grid from '@material-ui/core/Grid';
import withRoot from '../withRoot';
import TraceTable from '../components/TraceTable';
import TraceEventDetails from '../components/TraceEventDetails';

const styles = (theme: Theme) =>
  createStyles({
    root: {
      
    },
    tableContainer: {
      height: 320,
    },
    paper: {
      padding: 0,
      textAlign: 'center',
      color: theme.palette.text.secondary,
    },
  });

type State = {
  selectedTrace: InEventTrace | OutEventTrace | ExceptionTrace;
  selectedTraces: Trace[]
};

interface Props {
  store: any;
};

@observer
class Dashboard extends React.Component<Props & WithStyles<typeof styles>, State> {
  state = {
    selectedTrace: undefined,
    selectedTraces: []
  };

  onTraceSelect = (trace: InEventTrace | OutEventTrace | ExceptionTrace) => {
    console.log("trace select", trace)
    if(this.state.selectedTraces.indexOf(trace) == -1)
      this.setState({...this.state, selectedTraces: [...this.state.selectedTraces, trace], selectedTrace: trace})
    else
      this.setState({...this.state, selectedTraces: this.state.selectedTraces.filter(x => x != trace), selectedTrace: trace })
  }

  render() {    
    //const eventStream = this.props.store.eventStreamStore.events
    const  classes = this.props.classes;
    return (
      <div className={this.props.classes.root}> 
      <Grid container spacing={24}>
        <Grid item xs={12}>
          <Paper className={classes.paper}>
            <ServiceVisualizer store={this.props.store} selected={this.state.selectedTraces}  />
          </Paper>
        </Grid>        
        <Grid item xs={6}>
          <TraceTable store={this.props.store} onSelect={this.onTraceSelect} multiSelect={true} />
        </Grid>
        <Grid item xs={6}>
          {this.state.selectedTrace && <TraceEventDetails trace={this.state.selectedTrace} store={this.props.store} />}            
        </Grid>        
      </Grid>              
      </div>
    );
  }
}

export default withRoot(withStyles(styles)(Dashboard));