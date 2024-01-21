import classNames from "classnames";
import React, { ReactNode } from "react";

export const Button = (props: {
  disabled?: boolean;
  onClick?: React.MouseEventHandler<HTMLButtonElement>;
  onMouseDown?: React.MouseEventHandler<HTMLButtonElement>;
  margin?: string;
  padding?: string;
  children: ReactNode;
}) => {
  return (
    <button
      type="button"
      disabled={props.disabled}
      onClick={props.onClick}
      onMouseDown={props.onMouseDown}
      className={classNames(
        `p-${props.padding || "2"}`,
        `m-${props.margin || "2"}`,
        "rounded bg-white text-black hover:brightness-75",
      )}
    >
      {props.children}
    </button>
  );
};
