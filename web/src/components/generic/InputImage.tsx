import React from "react";
import { twMerge } from "tailwind-merge";

export const InputImage = (props: {
  src: string;
  alt: string;
  disabled: boolean;
  onClick: React.MouseEventHandler<HTMLInputElement>;
  className?: string;
}) => {
  return (
    <input
      type="image"
      src={props.src}
      alt={props.alt}
      disabled={props.disabled}
      onClick={props.onClick}
      className={twMerge("w-8 h-8", props.className)}
    />
  );
};
