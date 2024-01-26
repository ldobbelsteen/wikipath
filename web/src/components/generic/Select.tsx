import React, { ReactNode } from "react";
import { twMerge } from "tailwind-merge";

export const Select = (props: {
  label: string;
  value: string;
  disabled: boolean;
  onChange: React.ChangeEventHandler<HTMLSelectElement>;
  options: { value: string; children: ReactNode }[];
  optionClassName?: string;
  className?: string;
}) => {
  return (
    <select
      aria-label={props.label}
      value={props.value}
      disabled={props.disabled}
      onChange={props.onChange}
      className={twMerge(
        "p-2 m-2 rounded bg-white text-black hover:brightness-75 cursor-pointer",
        props.className,
      )}
    >
      {props.options.map((option, i) => (
        <option
          key={i}
          value={option.value}
          className={twMerge("cursor-pointer", props.optionClassName)}
        >
          {option.children}
        </option>
      ))}
    </select>
  );
};
