import React, { useState, useEffect, useRef } from "react";
import Autosuggest from "react-autosuggest";
import PropTypes from "prop-types";

import * as theme from "./Form.module.scss";
// eslint-disable-next-line import/no-unresolved
import Dice from "url:./dice.svg";
// eslint-disable-next-line import/no-unresolved
import Swap from "url:./swap.svg";

import {
  getAvailableDatabases,
  getUrlParameter,
  getRandomTitle,
  getSuggestions,
} from "../api";

const Button = (props) => {
  return (
    <button disabled={props.disabled} onClick={props.onClick}>
      {props.text}
    </button>
  );
};

const Input = (props) => {
  const [suggestions, setSuggestions] = useState([]);
  const latestFetch = useRef(null);

  const updateSuggestions = ({ value }) => {
    const controller = (latestFetch.current = new AbortController());
    getSuggestions(value, props.language).then((result) => {
      if (latestFetch.current !== null) latestFetch.current.abort();
      if (latestFetch.current === controller) setSuggestions(result);
    });
  };

  return (
    <div className="autosuggest">
      <Autosuggest
        theme={theme}
        suggestions={suggestions}
        getSuggestionValue={(d) => d}
        onSuggestionsFetchRequested={updateSuggestions}
        onSuggestionsClearRequested={() => setSuggestions([])}
        renderSuggestion={(suggestion) => <span>{suggestion}</span>}
        inputProps={{
          value: props.value,
          disabled: props.disabled,
          placeholder: props.placeholder,
          style: props.notFound ? { backgroundColor: "#ff8c8c" } : {},
          onChange: (_, { newValue }) => props.handler(newValue),
        }}
      />
      <input
        className="random"
        type="image"
        src={Dice}
        alt="Get random page"
        disabled={props.disabled}
        onClick={() => getRandomTitle(props.language).then(props.handler)}
      ></input>
    </div>
  );
};

const Select = ({ disabled, handler }) => {
  const [index, setIndex] = useState(0);
  const [options, setOptions] = useState([]);

  useEffect(() => {
    getAvailableDatabases().then((data) => {
      setOptions(data);
      if (data.length === 0) {
        handler({ language: "none", date: "none", code: "none" });
      } else {
        let index = data.findIndex(
          (element) => element.code === getUrlParameter("language")
        );
        if (index < 0) {
          const userLanguages = navigator.languages || [navigator.language];
          for (let language of userLanguages) {
            index = data.findIndex((database) =>
              database.code.includes(language.substring(0, 2))
            );
            if (index >= 0) {
              break;
            }
          }
          if (index < 0) {
            index = 0;
          }
        }
        setIndex(index);
        handler(data[index]);
      }
    });
  }, [handler]);

  return (
    <select
      value={index}
      disabled={disabled}
      onBlur={(event) => {
        handler(options[event.target.value]);
        setIndex(event.target.value);
      }}
      onChange={(event) => {
        handler(options[event.target.value]);
        setIndex(event.target.value);
      }}
    >
      {options.map((option, index) => (
        <option key={index} value={index}>
          {option.language}
        </option>
      ))}
    </select>
  );
};

const Form = (props) => {
  const [database, setDatabase] = useState({});
  const [sourceInput, setSourceInput] = useState(
    getUrlParameter("source") || ""
  );
  const [targetInput, setTargetInput] = useState(
    getUrlParameter("target") || ""
  );
  const [sourceNotFound, setSourceNotFound] = useState(false);
  const [targetNotFound, setTargetNotFound] = useState(false);

  function swap() {
    const temp = sourceInput;
    setSourceInput(targetInput);
    setTargetInput(temp);
    setSourceNotFound(false);
    setTargetNotFound(false);
  }

  return (
    <div className="form">
      <p>Find the shortest path between any two Wikipedia pages</p>
      <div className="subform">
        <Select disabled={props.isBusy} handler={setDatabase} />
        <Input
          disabled={props.isBusy}
          value={sourceInput}
          notFound={sourceNotFound}
          handler={(value) => {
            setSourceInput(value);
            setSourceNotFound(false);
          }}
          placeholder={"Starting page"}
          language={database.code}
        />
        <input
          className="swap"
          type="image"
          src={Swap}
          alt="Get random page"
          disabled={props.isBusy}
          onClick={swap}
        ></input>
        <Input
          disabled={props.isBusy}
          value={targetInput}
          notFound={targetNotFound}
          handler={(value) => {
            setTargetInput(value);
            setTargetNotFound(false);
          }}
          placeholder={"End page"}
          language={database.code}
        />
        <Button
          disabled={props.isBusy}
          text="Find!"
          onClick={async () => {
            const error = await props.search(
              sourceInput,
              targetInput,
              database.code
            );
            if (error === "source") {
              setSourceNotFound(true);
            } else {
              setSourceNotFound(false);
            }
            if (error === "target") {
              setTargetNotFound(true);
            } else {
              setTargetNotFound(false);
            }
          }}
        />
      </div>
    </div>
  );
};

export default Form;

Button.propTypes = {
  text: PropTypes.string,
  disabled: PropTypes.bool,
  onClick: PropTypes.func,
};

Input.propTypes = {
  value: PropTypes.string,
  disabled: PropTypes.bool,
  placeholder: PropTypes.string,
  handler: PropTypes.func,
  language: PropTypes.string,
  notFound: PropTypes.bool,
};

Select.propTypes = {
  disabled: PropTypes.bool,
  handler: PropTypes.func,
};

Form.propTypes = {
  isBusy: PropTypes.bool,
  search: PropTypes.func,
};
