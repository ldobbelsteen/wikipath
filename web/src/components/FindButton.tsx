import React from "react";

export default function FindButton(props: {
  disabled: boolean;
  onClick: () => void;
  text: string;
}): JSX.Element {
  return (
    <button disabled={props.disabled} onClick={props.onClick}>
      {props.text}
    </button>
  );
}
