import * as React from 'react';
import { ServiceDescriptor, ServiceMethod } from '../stores/data'
import { Theme } from '@material-ui/core/styles/createMuiTheme';
import classnames from 'classnames';
import createStyles from '@material-ui/core/styles/createStyles';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import withRoot from '../withRoot';
import Card from '@material-ui/core/Card';
import CardHeader from '@material-ui/core/CardHeader';
import CardMedia from '@material-ui/core/CardMedia';
import CardContent from '@material-ui/core/CardContent';
import CardActions from '@material-ui/core/CardActions';
import Collapse from '@material-ui/core/Collapse';
import Avatar from '@material-ui/core/Avatar';
import IconButton from '@material-ui/core/IconButton';
import Typography from '@material-ui/core/Typography';
import red from '@material-ui/core/colors/red';
import FavoriteIcon from '@material-ui/icons/Favorite';
import ShareIcon from '@material-ui/icons/Share';
import ExpandMoreIcon from '@material-ui/icons/ExpandMore';
import MoreVertIcon from '@material-ui/icons/MoreVert';
import List from '@material-ui/core/List';
import ListItem from '@material-ui/core/ListItem';
import ListItemText from '@material-ui/core/ListItemText';
import CardActionArea from '@material-ui/core/CardActionArea';

const styles = (theme: Theme) =>
  createStyles({
    card: {
        maxWidth: 800,
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
    expanded: boolean
};

interface Props {
  serviceDescriptor: ServiceDescriptor;  
  expanded?: boolean;
  onMethodSelect: (MethodDescriptor) => void;
  onServiceSelect?: (ServiceDescriptor) => void;
};

class ServiceCard extends React.Component<Props & WithStyles<typeof styles>, State> {
    state = { expanded: this.props.expanded };

    handleExpandClick = () => {
      this.setState(state => ({ expanded: !state.expanded }));
    };
    onMethodSelect = (x: ServiceMethod) => {
        console.log("onMethodSelect")
        this.props.onMethodSelect(x);
    }
    selectServiceType = () =>{
        if(this.props.onServiceSelect)
            this.props.onServiceSelect(this.props.serviceDescriptor)
    }

  render() {    
    const classes = this.props.classes 
    const sd = this.props.serviceDescriptor
    return (
        <Card className={classes.card}>          
        <CardHeader
          avatar={
            <Avatar aria-label="Service" className={classes.avatar}>
              S
            </Avatar>
          }
          action={
            <IconButton>
              <MoreVertIcon />
            </IconButton>
          }
          title={sd.service}
          subheader={sd.upTime}
        />
        <CardActionArea onClick={this.selectServiceType}>
        <CardMedia
          className={classes.media}
          image={"/imgs/"+sd.service+".png"}
          title={sd.service}
        />
        <CardContent>
          <Typography component="p">
            This impressive paella is a perfect party dish and a fun meal to cook together with your
            guests. Add 1 cup of frozen peas along with the mussels, if you like.
            
          </Typography>
        </CardContent>
        </CardActionArea>
        <CardActions className={classes.actions} disableActionSpacing>
          <IconButton aria-label="Add to favorites">
            <FavoriteIcon />
          </IconButton>        
          <IconButton
            className={classnames(classes.expand, {
              [classes.expandOpen]: this.state.expanded,
            })}
            onClick={this.handleExpandClick}
            aria-expanded={this.state.expanded}
            aria-label="Show more"
          >
            <ExpandMoreIcon />
          </IconButton>
        </CardActions>
        <Collapse in={this.state.expanded} timeout="auto" unmountOnExit>
          <CardContent>
            <Typography paragraph>Methods:</Typography>
            <Typography paragraph>
              Click on a method to trigger an event of that type:
            </Typography>
            <div>
                <List>
                { sd.serviceMethods.map( (x: ServiceMethod ) => (
                    <ListItem key={x.in.fqn} button={true} onClick={() => this.onMethodSelect(x) } >
                        <ListItemText                      
                        primary={x.in.fqn}
                        secondary={x.out.fqn}
                        />
                    </ListItem>)                
                )}
              </List> 
            </div>
          </CardContent>
        </Collapse>
      </Card>
    );
  }
}

export default withRoot(withStyles(styles)(ServiceCard));