import React from "react";

export default function SourceCode(props: { url: string }): JSX.Element {
  return (
    <div className="sourcecode">
      <a href={props.url}>Source code</a>
    </div>
  );
}
