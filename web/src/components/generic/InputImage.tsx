import classNames from "classnames";
import React from "react";

export const InputImage = (props: {
  src: string;
  alt: string;
  disabled: boolean;
  onClick: React.MouseEventHandler<HTMLInputElement>;
  className: string;
}) => {
  return (
    <input
      type="image"
      src={props.src}
      alt={props.alt}
      disabled={props.disabled}
      onClick={props.onClick}
      className={classNames("m-0 p-0", props.className)}
    />
  );
};
