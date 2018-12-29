import * as React from 'react';
import * as d3 from 'd3';
import {d3Link} from './data'
import { Theme } from '@material-ui/core/styles/createMuiTheme';
import createStyles from '@material-ui/core/styles/createStyles';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import withRoot from '../../withRoot';

const styles = (theme: Theme) =>
  createStyles({
    root: {
      textAlign: 'center'      
    },
  });


  export interface LinkProps{
    link: d3Link;
  }


class D3Link extends React.Component<LinkProps, undefined> {
  ref: SVGLineElement;

  componentDidMount() {
    d3.select(this.ref).data([this.props.link]);
  }

  render() {
    if(this.props.link.isSelected)
      return <line className="link" stroke="red" ref={(ref: SVGLineElement) => this.ref = ref} strokeWidth={this.props.link.value+0.5}/>;
    return <line className="link" stroke="#265692" ref={(ref: SVGLineElement) => this.ref = ref} strokeWidth={this.props.link.value}/>;
  }
}

export interface Props{
  links: d3Link[];
}

class D3Links extends React.Component<Props & WithStyles<typeof styles>, undefined> {
  render() {
    const links = this.props.links.map((link: d3Link, index: number) => {
      return <D3Link key={index} link={link}/>;
    });

    return (
      <g className="links">
        {links}
      </g>
    );
  }
}

export default withRoot(withStyles(styles)(D3Links));
