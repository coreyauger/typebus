import * as React from 'react';
import {observer} from 'mobx-react';
import AceEditor from 'react-ace';
import { ServiceMethod, PublishedEvent, ExceptionTrace, InEventTrace, OutEventTrace } from '../stores/data'
import { Theme } from '@material-ui/core/styles/createMuiTheme';
import createStyles from '@material-ui/core/styles/createStyles';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import Typography from '@material-ui/core/Typography'
import Card from '@material-ui/core/Card';
import MoreVertIcon from '@material-ui/icons/MoreVert';
import CardHeader from '@material-ui/core/CardHeader';
import CardContent from '@material-ui/core/CardContent';
import Tabs from '@material-ui/core/Tabs';
import Tab from '@material-ui/core/Tab';
import AppBar from '@material-ui/core/AppBar';
import red from '@material-ui/core/colors/red';
import Avatar from '@material-ui/core/Avatar';
import IconButton from '@material-ui/core/IconButton';
import Divider from '@material-ui/core/Divider';
import withRoot from '../withRoot';
import * as brace from 'brace';

import 'brace/mode/javascript';
import 'brace/theme/github';

interface NormalizedTraceEvent{
  service: string,
  serviceId: string,
  event: PublishedEvent,
  time?: number,
  id?: string
}

const styles = (theme: Theme) =>
  createStyles({
    root: {
      flexGrow: 1
    },
    card: {
    },
    media: {
      height: 0,
      paddingTop: '56.25%', // 16:9
    },
    actions: {
      display: 'flex',
    },
    expand: {
      transform: 'rotate(0deg)',
      transition: theme.transitions.create('transform', {
        duration: theme.transitions.duration.shortest,
      }),
      marginLeft: 'auto',
      [theme.breakpoints.up('sm')]: {
        marginRight: -8,
      },
    },
    expandOpen: {
      transform: 'rotate(180deg)',
    },
    avatar: {
      backgroundColor: red[500],
    },
  });

type State = {
  tab: number;
  selectedMethod: ServiceMethod;  
};

interface Props {
  store: any;  
  trace: InEventTrace | OutEventTrace | ExceptionTrace;
};

@observer
class TraceEventDetails extends React.Component<Props & WithStyles<typeof styles>, State> {
  state = {
    tab: 0,
    selectedMethod: undefined    
  };

  handleTabChange = (event, tab) => {
    this.setState({...this.state, tab });
  };
 
  render() {   
    // CA - might be cool to know where this event came from.. and track it back to the last place it came from?
    // * need to think more what this would look like.. 
    const trace = this.props.trace 
    if(!trace)return <div className={this.props.classes.root}></div>;

    console.log("schemaz", this.props.store.serviceStore.schema)
    console.log("lookup: ", trace.event.meta.eventType)
    const schema = this.props.store.serviceStore.schema[trace.event.meta.eventType]
    const payload = schema.fromBuffer( Buffer.from(trace.event.payload) )
    
    const  classes = this.props.classes;
    return (<div className={this.props.classes.root}>  


     <Card className={classes.card}>
        <CardHeader
          avatar={
            <Avatar aria-label="Recipe" className={classes.avatar}>
              Ev
            </Avatar>
          }
          action={
            <IconButton>
              <MoreVertIcon />
            </IconButton>
          }
          title={trace.event.meta.eventType}
          subheader={trace.event.meta.occurredAt}
        />
             
        <CardContent>
        <Divider />
          <pre>
            {JSON.stringify(trace.event.meta,null,'\t')}
          </pre>          
        <Divider />      
        </CardContent>
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

         <AceEditor
              height="700px"
              width="1200px"
              mode="javascript"
              theme="github"            
              setOptions={ {readOnly: true} }
              name={trace.event.meta.eventType}
              editorProps={{$blockScrolling: true}}
              value={(this.state.tab == 0 ? JSON.stringify(payload,null,'\t') : JSON.stringify(JSON.parse( '{ "TODO":1 }' ),null,'\t') )}
            />    
                
      </Card>
      {/*} <TriggerEventDialog open={this.state.selectedMethod != undefined} method={this.state.selectedMethod} onClose={this.clearMethodSelect} onTriggerEvent={this.onTriggerEvent} />  */ }
      </div>);       
  }
}

export default withRoot(withStyles(styles)(TraceEventDetails));