import React, { ReactElement } from "react";

export const Select = (props: {
  value: string;
  disabled: boolean;
  onChange: React.ChangeEventHandler<HTMLSelectElement>;
  options: ReactElement[];
}) => {
  return (
    <select
      value={props.value}
      disabled={props.disabled}
      onChange={props.onChange}
      className="p-2 m-2 rounded bg-white text-black hover:brightness-75"
    >
      {props.options}
    </select>
  );
};
