import React, { useEffect, useRef, useState } from "react";
import { SimulationLinkDatum, SimulationNodeDatum } from "d3-force";
import {
  drag,
  forceCenter,
  forceLink,
  forceManyBody,
  forceSimulation,
  scaleOrdinal,
  schemeCategory10,
  select,
  zoom,
} from "d3";
import { Graph } from "../helpers/api";
import Loading from "../static/loading.svg";
import { pseudoRandomShuffle } from "../helpers/misc";

type Link = SimulationLinkDatum<Node>;
interface Node extends SimulationNodeDatum {
  id: number;
  title: string;
  degree: number;
}

export default function PathsGraph(props: {
  graph: Graph | undefined;
  isLoading: boolean;
  maxPaths: number;
}): JSX.Element {
  const ref = useRef<SVGSVGElement>(null);
  const [text, setText] = useState("");

  // Re-render on data change
  useEffect(() => {
    if (props.isLoading) return;
    if (ref.current === null) return;
    const svg = select(ref.current);
    svg.attr("width", "100%").attr("height", "100%");
    svg.selectAll("*").remove();

    const { graph } = props;
    if (!graph) return;

    if (graph.pathCount === 0) {
      setText("No path found");
      return;
    }

    // Show message based on graph content
    let message = `Found ${graph.pathCount} ${
      graph.pathCount === 1 ? "path" : "paths"
    } of degree ${graph.pathDegree} in ${
      Math.round(graph.searchDuration / 10) / 100
    } seconds`;
    if (graph.pathCount > props.maxPaths) {
      message += `. Only ${props.maxPaths} of them are shown below`;
    }
    setText(message);

    // Extract paths from the graph. If there are more than maxPaths, limit
    // the number selected by randomly (but deterministically) selecting only
    // the first n paths
    const paths: number[][] = [];
    const extractPaths = (page: number, path: number[]): boolean => {
      let outgoing = graph.outgoingLinks[page];
      if (outgoing && outgoing.length > 0) {
        outgoing = pseudoRandomShuffle(outgoing);
        for (let i = 0; i < outgoing.length; i++) {
          const maxReached = extractPaths(outgoing[i], [...path, outgoing[i]]);
          if (maxReached) {
            return true;
          }
        }
      } else {
        paths.push(path);
        if (paths.length >= props.maxPaths) return true;
      }
      return false;
    };
    extractPaths(graph.sourcePage, [graph.sourcePage]);

    // Extract nodes and links for D3 from the paths
    const nodes: Node[] = [];
    const links: Link[] = [];
    paths.forEach((path) => {
      let previousNode: Node;
      path.forEach((id, index) => {
        let currentNode = nodes.find((node) => node.id === id);
        if (!currentNode) {
          currentNode = {
            id: id,
            title: graph.pageNames[id],
            degree: index,
          };
          nodes.push(currentNode);
        }
        if (index != 0) {
          links.push({
            source: previousNode,
            target: currentNode,
          });
        }
        previousNode = currentNode;
      });
    });

    // Force simulation; gravitate to center and gravitate away from eachother
    const centerX = 0.5 * (ref?.current?.clientWidth || 0);
    const centerY = 0.5 * (ref?.current?.clientHeight || 0);
    const simulation = forceSimulation(nodes)
      .force("link", forceLink(links))
      .force("charge", forceManyBody().strength(-2000).distanceMax(300))
      .force("center", forceCenter(centerX, centerY));

    // Add link arrow head definition to the svg
    svg
      .append("svg:defs")
      .selectAll("marker")
      .data(["arrowhead"])
      .enter()
      .append("svg:marker")
      .attr("id", String)
      .attr("viewBox", "0 -5 10 10")
      .attr("refX", 19)
      .attr("markerWidth", 5)
      .attr("markerHeight", 5)
      .attr("orient", "auto")
      .append("svg:path")
      .attr("d", "M0,-5L10,0L0,5");

    // Create group for the links
    const link = svg
      .append("g")
      .attr("class", "links")
      .selectAll("line")
      .data(links)
      .enter()
      .append("line")
      .attr("stroke", "black")
      .attr("stroke-width", 2)
      .attr("marker-end", "url(#arrowhead)");

    // Allow dragging nodes and their titles
    const nodeDrag: d3.DragBehavior<
      SVGGElement,
      Node,
      d3.SubjectPosition | Node
    > = drag();
    nodeDrag.on("start", (event) => {
      if (!event.active) simulation.alphaTarget(0.3).restart();
      event.subject.fx = event.subject.x;
      event.subject.fy = event.subject.y;
    });
    nodeDrag.on("drag", (event) => {
      event.subject.fx = event.x;
      event.subject.fy = event.y;
    });
    nodeDrag.on("end", (event) => {
      if (!event.active) simulation.alphaTarget(0);
      event.subject.fx = null;
      event.subject.fy = null;
    });

    // Add zoom and pan behaviour
    svg.call(
      zoom<SVGSVGElement, unknown>()
        .on("zoom", ({ transform }) => {
          select(".pages").attr("transform", transform);
          select(".links").attr("transform", transform);
        })
        .scaleExtent([0.5, 4])
    );

    // Create group for the pages/nodes
    const node = svg
      .append("g")
      .attr("class", "pages")
      .selectAll("g")
      .data(nodes)
      .enter()
      .append("g")
      .call(nodeDrag);

    // Make nodes clickable, opening a tab to the corresponding Wikipedia article
    const clickable = node
      .append("a")
      .attr("target", "_blank")
      .attr(
        "href",
        (d) => `https://${graph.languageCode}.wikipedia.org/wiki/${d.title}`
      );

    // Represent the nodes as colored circles
    const colors = scaleOrdinal(schemeCategory10);
    clickable
      .append("circle")
      .attr("r", 10)
      .attr("fill", (d) => colors(d.degree.toString()))
      .attr("stroke", "white")
      .attr("stroke-width", 2);

    // Add the title corresponding to the node's page
    clickable
      .append("text")
      .text((d) => d.title)
      .attr("style", "user-select: none")
      .attr("x", 16)
      .attr("y", 5);

    // Start physics simulation
    simulation.on("tick", () => {
      link
        .attr("x1", (d: Link) => (d.source as Node).x?.toString() || "")
        .attr("y1", (d: Link) => (d.source as Node).y?.toString() || "")
        .attr("x2", (d: Link) => (d.target as Node).x?.toString() || "")
        .attr("y2", (d: Link) => (d.target as Node).y?.toString() || "");
      node.attr("transform", (d) => `translate(${d.x},${d.y})`);
    });
  }, [props]);

  // Show graph, loading or nothing based on props
  const view = props.isLoading ? (
    <img src={Loading} alt="Loading..."></img>
  ) : props.graph ? (
    <>
      <p>{text}</p>
      <svg ref={ref}></svg>
    </>
  ) : (
    <></>
  );

  return <div className="graph">{view}</div>;
}
