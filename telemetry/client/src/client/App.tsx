import * as React from 'react';
import { Route } from 'react-router';
import classNames from 'classnames';
import {observer, inject} from 'mobx-react';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import CssBaseline from '@material-ui/core/CssBaseline';
import Drawer from '@material-ui/core/Drawer';
import AppBar from '@material-ui/core/AppBar';
import Toolbar from '@material-ui/core/Toolbar';
import List from '@material-ui/core/List';
import Typography from '@material-ui/core/Typography';
import Divider from '@material-ui/core/Divider';
import IconButton from '@material-ui/core/IconButton';
import Badge from '@material-ui/core/Badge';
import MenuIcon from '@material-ui/icons/Menu';
import ChevronLeftIcon from '@material-ui/icons/ChevronLeft';
import NotificationsIcon from '@material-ui/icons/Notifications';
import { Theme } from '@material-ui/core/styles/createMuiTheme';
import createStyles from '@material-ui/core/styles/createStyles';
import withRoot from './withRoot';
import stores from './stores/index';

import ListItem from '@material-ui/core/ListItem';
import ListItemIcon from '@material-ui/core/ListItemIcon';
import ListItemText from '@material-ui/core/ListItemText';
import ListSubheader from '@material-ui/core/ListSubheader';
import DashboardIcon from '@material-ui/icons/Dashboard';
import PeopleIcon from '@material-ui/icons/People';
import BarChartIcon from '@material-ui/icons/BarChart';
import LayersIcon from '@material-ui/icons/Layers';
import AssignmentIcon from '@material-ui/icons/Assignment';
import Dashboard from './pages/Dashboard';
import Services from './pages/Services';
import {TypebusSocket} from './socket/WebSocket'



const drawerWidth = 240;

const styles = (theme: Theme) =>
createStyles({
  root: {
    display: 'flex',
  },
  toolbar: {
    paddingRight: 24, // keep right padding when drawer closed
  },
  toolbarIcon: {
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'flex-end',
    padding: '0 8px',
    ...theme.mixins.toolbar,
  },
  appBar: {
    zIndex: theme.zIndex.drawer + 1,
    transition: theme.transitions.create(['width', 'margin'], {
      easing: theme.transitions.easing.sharp,
      duration: theme.transitions.duration.leavingScreen,
    }),
  },
  appBarShift: {
    marginLeft: drawerWidth,
    width: `calc(100% - ${drawerWidth}px)`,
    transition: theme.transitions.create(['width', 'margin'], {
      easing: theme.transitions.easing.sharp,
      duration: theme.transitions.duration.enteringScreen,
    }),
  },
  menuButton: {
    marginLeft: 12,
    marginRight: 36,
  },
  menuButtonHidden: {
    display: 'none',
  },
  title: {
    flexGrow: 1,
  },
  drawerPaper: {
    position: 'relative',
    whiteSpace: 'nowrap',
    width: drawerWidth,
    transition: theme.transitions.create('width', {
      easing: theme.transitions.easing.sharp,
      duration: theme.transitions.duration.enteringScreen,
    }),
  },
  drawerPaperClose: {
    overflowX: 'hidden',
    transition: theme.transitions.create('width', {
      easing: theme.transitions.easing.sharp,
      duration: theme.transitions.duration.leavingScreen,
    }),
    width: theme.spacing.unit * 7,
    [theme.breakpoints.up('sm')]: {
      width: theme.spacing.unit * 9,
    },
  },
  appBarSpacer: theme.mixins.toolbar,
  content: {
    flexGrow: 1,
    padding: theme.spacing.unit * 3,
    height: '100vh',
    overflow: 'auto',    
    backgroundColor: "#f1f0f1",
  },  
  h5: {
    marginBottom: theme.spacing.unit * 2,
  },
  logo:{ 
      mark: {
        width: 36,
        height: 36,        
    }
  }
});

export const secondaryListItems = (
  <div>
    <ListSubheader inset>Saved tests</ListSubheader>
    <ListItem button>
      <ListItemIcon>
        <AssignmentIcon />
      </ListItemIcon>
      <ListItemText primary="PSR test" />
    </ListItem>
    <ListItem button>
      <ListItemIcon>
        <AssignmentIcon />
      </ListItemIcon>
      <ListItemText primary="Some test" />
    </ListItem>
    <ListItem button>
      <ListItemIcon>
        <AssignmentIcon />
      </ListItemIcon>
      <ListItemText primary="Another test" />
    </ListItem>
  </div>
);


type State = {
  open: boolean;
};

type Props = {
  store: any
}

@inject('routing')
@observer
class App extends React.Component<Props & WithStyles<typeof styles>, State> {
  state = {
    open: true,
  };
  typebusSocket: TypebusSocket = null 
  constructor(props){    
    super(props);
    props.store.socketStore.connect(new TypebusSocket("bc5e5dd1-fc80-4cfd-9a26-b8e60b43b8b2"))    
  }

  handleDrawerOpen = () => {
    this.setState({ open: true });
  };

  handleDrawerClose = () => {
    this.setState({ open: false });
  };

  render() {
    console.log("APP RENDER!!")
    const { classes } = this.props;    
    const { location, push, goBack } = stores.routing; // CA - inject above did not work.. you should see this as a "prop" (investigate)
    return (
      
      <React.Fragment>
        <CssBaseline />
        <div className={classes.root}>      
          <AppBar          
            position="absolute"
            className={classNames(classes.appBar, this.state.open && classes.appBarShift)}
          >            
            <Toolbar disableGutters={!this.state.open} className={classes.toolbar}>
            
              <IconButton
                color="inherit"
                aria-label="Open drawer"
                onClick={this.handleDrawerOpen}
                className={classNames(
                  classes.menuButton,
                  this.state.open && classes.menuButtonHidden,
                )}
              >               
                <MenuIcon />              
              </IconButton>
              <Typography
                component="h1"
                variant="h6"
                color="inherit"
                noWrap
                className={classes.title}
              >
                Dashboard
              </Typography>
              <IconButton color="inherit">
                <Badge badgeContent={4} color="secondary">
                  <NotificationsIcon />
                </Badge>
              </IconButton>
            </Toolbar>
          </AppBar>
          <Drawer
            variant="permanent"
            classes={{
              paper: classNames(classes.drawerPaper, !this.state.open && classes.drawerPaperClose),
            }}
            open={this.state.open}>
            <div className={classes.toolbarIcon}>
              <div style={ {position: "absolute", left: "25px"} }>
                    {/* TYPEBUS LOGO HERE */}                         
              </div>
              <IconButton onClick={this.handleDrawerClose}>
                <ChevronLeftIcon />
              </IconButton>
            </div>
            <Divider />
           <List>
           <div>
            <ListItem button>
              <ListItemIcon>
                <DashboardIcon />
              </ListItemIcon>
              <ListItemText primary="Dashboard" onClick={() => push("/p/home")} />
            </ListItem>             
            <ListItem button>
              <ListItemIcon>
                <BarChartIcon />
              </ListItemIcon>
              <ListItemText primary="Tests" onClick={() => push("/p/tests")} />
            </ListItem>
            <ListItem button>
              <ListItemIcon>
                <LayersIcon />
              </ListItemIcon>
              <ListItemText primary="Services" onClick={() => push("/p/services")} />
            </ListItem>
          </div>
           </List>
            <Divider />
            <List>{secondaryListItems}</List> 
          </Drawer>
          <main className={classes.content}>
            <div className={classes.appBarSpacer} />


            <Route path='/p/home' render={(props) => <Dashboard store={stores} /> }  />
            <Route path='/p/service/:id' render={(props) => <Services store={stores} service={props.id} /> }  />
            <Route path='/p/services' render={(props) => <Services store={stores} /> }  />                        

            
          </main>
        </div>
      </React.Fragment>
    );
  }
}

export default withRoot((withStyles(styles)(App) ));
