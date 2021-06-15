import React from "react";
import PropTypes from "prop-types";

const Header = (props) => {
  return (
    <div className="header">
      <a href="/">{props.text}</a>
    </div>
  );
};

export default Header;

Header.propTypes = {
  text: PropTypes.string,
};
