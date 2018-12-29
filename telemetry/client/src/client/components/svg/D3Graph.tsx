import * as React from 'react';
import * as d3 from 'd3';
import { Theme } from '@material-ui/core/styles/createMuiTheme';
import createStyles from '@material-ui/core/styles/createStyles';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import withRoot from '../../withRoot';
import D3Nodes from './D3Nodes';
import D3Links from './D3Links';
import { Graph, d3Node } from './data';

const styles = (theme: Theme) =>
  createStyles({
    root: {
      textAlign: 'center'      
    },
  });

export interface Props{
    width: number;
    height: number;    
    graph: Graph;    
}



class D3Graph extends React.Component<Props & WithStyles<typeof styles>, undefined> {
    ref: SVGSVGElement;
    simulation: any;

    constructor(props: Props & WithStyles<typeof styles>) {
        super(props);
        this.simulation = d3.forceSimulation()
          .force("link", d3.forceLink().id(function(d: d3Node) {
            return d.id;
          }))                    
          .force("charge", d3.forceManyBody().strength(-100))
          .force("center", d3.forceCenter(this.props.width, this.props.height / 2))
          .nodes(this.props.graph.nodes);
    
        this.simulation.force("link").links(this.props.graph.links);
      }

      componentDidMount() {
        const node = d3.select(".nodes").selectAll(".node");
        const link = d3.select(".links").selectAll("line");    
        this.simulation.nodes(this.props.graph.nodes).on("tick", ticked);    
        function ticked() {
          link
            .attr("x1", function(d: any) {
              return d.source.x;
            })
            .attr("y1", function(d: any) {
              return d.source.y;
            })
            .attr("x2", function(d: any) {
              return d.target.x;
            })
            .attr("y2", function(d: any) {
              return d.target.y;
            });
    
          node
            .attr("transform",  function(d: any) {
              return "translate("+d.x+","+d.y+")";
            })
        }
      }     
      componentWillUnmount(){
        this.simulation.stop()
      }   

  render() {
    
    return ( 
      <svg className="container" ref={(ref: SVGSVGElement) => this.ref = ref} width={this.props.width} height={this.props.height}>
          <D3Links links={this.props.graph.links}/>
          <D3Nodes nodes={this.props.graph.nodes} simulation={this.simulation} />
      </svg>    
    );
  }
}

export default withRoot(withStyles(styles)(D3Graph));
