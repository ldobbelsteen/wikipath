import classNames from "classnames";
import React from "react";

export const InputText = (props: {
  value: string;
  invalid: boolean;
  disabled: boolean;
  placeholder: string;
  onChange: React.ChangeEventHandler<HTMLInputElement>;
  onFocus: React.FocusEventHandler<HTMLInputElement>;
  onBlur: React.FocusEventHandler<HTMLInputElement>;
}) => {
  return (
    <input
      type="text"
      value={props.value}
      placeholder={props.placeholder}
      disabled={props.disabled}
      onChange={props.onChange}
      onFocus={props.onFocus}
      onBlur={props.onBlur}
      className={classNames(
        {
          "bg-red-400 text-white placeholder:text-gray-300": props.invalid,
          "bg-white text-black": !props.invalid,
        },
        "p-2 m-2 rounded ",
      )}
    />
  );
};
