import React, { useEffect, useState } from "react";
import { toast } from "react-hot-toast";
import { Database, Api } from "../api";
import { Select } from "./generic/Select";

/** Select the best language based on the user's browser languages. */
export const defaultDatabase = (databases: Database[]) => {
  for (const browserLanguage of navigator.languages) {
    for (const database of databases) {
      if (database.languageCode.includes(browserLanguage.substring(0, 2))) {
        return database;
      }
    }
  }
  return databases[0];
};

export const DatabaseSelect = (props: {
  disabled: boolean;
  selected: Database | undefined;
  setSelected: (v: Database | undefined) => void;
}) => {
  const [databases, setDatabases] = useState<Database[]>();

  const { setSelected } = props;

  /**
   * Fetch available databases and select default.
   */
  useEffect(() => {
    Api.listDatabases()
      .then((databases) => {
        setDatabases(databases);
        setSelected(defaultDatabase(databases));
        return null;
      })
      .catch((err) => {
        toast.error(
          "An unexpected error occurred while getting the available languages",
        );
        console.error(err);
      });
  }, [setSelected]);

  return (
    databases !== undefined &&
    props.selected !== undefined && (
      <Select
        disabled={props.disabled}
        value={props.selected.languageCode}
        onChange={(ev) =>
          setSelected(
            databases.find(
              (database) => database.languageCode === ev.target.value,
            ),
          )
        }
        options={databases.map((database, index) => (
          <option key={index} value={database.languageCode}>
            {database.languageCode}
          </option>
        ))}
      />
    )
  );
};
