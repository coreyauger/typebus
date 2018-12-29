import * as React from 'react';
import * as d3 from 'd3';
import {d3Node} from './data'
import { Theme } from '@material-ui/core/styles/createMuiTheme';
import createStyles from '@material-ui/core/styles/createStyles';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import withRoot from '../../withRoot';

const styles = (theme: Theme) =>
  createStyles({
    root: {
      textAlign: 'center'      
    },
    label:{
      pointerEvents:"none",
      font: "10px sans-serif"
    }
  });

export interface Props{
    nodes: d3Node[];
    simulation: any;    
}

class D3Nodes extends React.Component<Props & WithStyles<typeof styles>, undefined> {
  ref: SVGGElement;
 
  componentDidMount(){
    const context: any = d3.select(this.ref);
    const simulation = this.props.simulation;
    const color = d3.scaleOrdinal(d3.schemeCategory10);
    
    //context.selectAll(".node").remove();
    const g = context.selectAll("node")
      .data(this.props.nodes)
      .enter()
      .append("g")
      .attr('class', "node")           
    g.append("circle")
      .attr("r", function(d: d3Node) {
        return d.radius;
      })
      .attr("fill", function(d: d3Node) {
        return color(d.group + "");
      }).call(d3.drag()
          .on("start", onDragStart)
          .on("drag", onDrag)
          .on("end", onDragEnd)); 
                
    g.append("text")
      .attr("dx", 12)
      .attr("dy", ".35em")
      .attr("class", this.props.classes.label)
      .text(function(d) { return d.name });
 
    function onDragStart(d: any) {
      if (!d3.event.active) {
        simulation.alphaTarget(0.3).restart();
      }
      d.fx = d.x;
      d.fy = d.y;
    }

    function onDrag(d: any) {
      d.fx = d3.event.x;
      d.fy = d3.event.y;
    }

    function onDragEnd(d: any) {
      if (!d3.event.active) {
        simulation.alphaTarget(0);
      }
      d.fx = null;
      d.fy = null;
    }
   
  }

  render() {
    return <g className="nodes" ref={(ref: SVGGElement) => this.ref = ref}/>;
  }
}

export default withRoot(withStyles(styles)(D3Nodes));
