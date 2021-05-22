import React, { useState, useRef, useEffect } from "react";
import PropTypes from "prop-types";
import * as d3 from "d3";

// eslint-disable-next-line import/no-unresolved
import Loading from "url:./loading.svg";

const maxShownPaths = 8;

// Run a deterministic pseudo random shuffle on an array, in-place
const pseudoRandomShuffle = (array) => {
  let seed = 1;

  function pseudoRandom() {
    const x = Math.sin(seed++) * 10000;
    return x - Math.floor(x);
  }

  for (let i = array.length - 1; i > 0; i--) {
    const j = Math.floor(pseudoRandom() * (i + 1));
    [array[i], array[j]] = [array[j], array[i]];
  }

  return array;
};

const Graph = (props) => {
  const ref = useRef(null);
  const [text, setText] = useState("");

  useEffect(() => {
    const svg = d3
      .select(ref.current)
      .attr("width", "100%")
      .attr("height", "100%")
      .call(
        d3.zoom().on("zoom", ({ transform }) => {
          d3.select(".links").attr("transform", transform);
          d3.select(".articles").attr("transform", transform);
        })
      );
    svg.selectAll("*").remove();

    if (props.data.paths.length === 0) {
      setText("No path found");
      return;
    }

    let message = `Found ${props.data.paths.length} ${
      props.data.paths.length === 1 ? "path" : "paths"
    } of degree ${props.data.paths[0].length - 1} in ${
      Math.round(props.data.time / 10) / 100
    } seconds`;
    if (props.data.paths.length > maxShownPaths) {
      message += `. Only ${maxShownPaths} of them are shown below`;
    }
    setText(message);

    const articles = [];
    const links = [];

    pseudoRandomShuffle(props.data.paths)
      .slice(0, maxShownPaths)
      .forEach((path) => {
        path.forEach((article, index) => {
          if (!articles.some((node) => node.title === article)) {
            articles.push({
              title: article,
              degree: index,
            });
          }
          if (index != 0) {
            links.push({
              source: path[index - 1],
              target: article,
            });
          }
        });
      });

    const color = d3.scaleOrdinal(d3.schemeCategory10);

    const simulation = d3
      .forceSimulation(articles)
      .force(
        "link",
        d3.forceLink(links).id((d) => d.title)
      )
      .force("charge", d3.forceManyBody().strength(-2000).distanceMax(300))
      .force(
        "center",
        d3
          .forceCenter(
            ref.current.clientWidth / 2,
            ref.current.clientHeight / 2
          )
          .strength(0.5)
      );

    svg
      .append("svg:defs")
      .selectAll("marker")
      .data(["arrowhead"])
      .enter()
      .append("svg:marker")
      .attr("id", String)
      .attr("viewBox", "0 -5 10 10")
      .attr("refX", 22)
      .attr("markerWidth", 4)
      .attr("markerHeight", 6)
      .attr("orient", "auto")
      .append("svg:path")
      .attr("d", "M0,-5L10,0L0,5");

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

    const dragging = (simulation) => {
      function dragstarted(event) {
        if (!event.active) simulation.alphaTarget(0.3).restart();
        event.subject.fx = event.subject.x;
        event.subject.fy = event.subject.y;
      }

      function dragged(event) {
        event.subject.fx = event.x;
        event.subject.fy = event.y;
      }

      function dragended(event) {
        if (!event.active) simulation.alphaTarget(0);
        event.subject.fx = null;
        event.subject.fy = null;
      }

      return d3
        .drag()
        .on("start", dragstarted)
        .on("drag", dragged)
        .on("end", dragended);
    };

    const article = svg
      .append("g")
      .attr("class", "articles")
      .selectAll("g")
      .data(articles)
      .enter()
      .append("g")
      .call(dragging(simulation));

    const clickable = article
      .append("a")
      .attr("target", "_blank")
      .attr(
        "href",
        (d) => `https://${props.data.language}.wikipedia.org/wiki/${d.title}`
      );

    clickable
      .append("circle")
      .attr("r", 10)
      .attr("fill", (d) => color(d.degree))
      .attr("stroke", "white")
      .attr("stroke-width", 2);

    clickable
      .append("text")
      .text((d) => d.title)
      .attr("style", "user-select: none")
      .attr("x", 16)
      .attr("y", 5);

    simulation.nodes(articles).on("tick", () => {
      link
        .attr("x1", (d) => d.source.x)
        .attr("y1", (d) => d.source.y)
        .attr("x2", (d) => d.target.x)
        .attr("y2", (d) => d.target.y);

      article.attr("transform", (d) => "translate(" + d.x + "," + d.y + ")");
    });
  }, [props.data]);

  return (
    <div className="graph">
      {props.isBusy ? (
        <img src={Loading} alt="Loading..."></img>
      ) : props.data.language ? (
        <>
          <p>{text}</p>
          <svg ref={ref}></svg>
        </>
      ) : (
        <></>
      )}
    </div>
  );
};

export default Graph;

Graph.propTypes = {
  isBusy: PropTypes.bool,
  data: PropTypes.shape({
    paths: PropTypes.array,
    language: PropTypes.string,
    time: PropTypes.number,
  }),
};
