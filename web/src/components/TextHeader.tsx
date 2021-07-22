import React from "react";

export default function TextHeader(props: { text: string }): JSX.Element {
  return (
    <header>
      <a href="/">{props.text}</a>
    </header>
  );
}
