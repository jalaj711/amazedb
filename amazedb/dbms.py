"""
The amazedb.dbms module is used to create, update, read from and write to the
AmazeDB databases stored in the 'db' directory.

Following functions can be used:

- dbms.create(name):
    Creates a new database of the name 'name'.

- dbms.db(name):
    Open a db of the name 'name' for reading and writing.

- dbms.getAllDbs():
    Returns the names of all the DBs available here.
"""

import os  # os module is required to manipulate directories and files
import json  # JSON module helps to manipulate json data
import re  # RegExp support for our app
import shutil  # This module helps to delete databases

# This module helps us to encrypt and decrypt our files
from cryptography.fernet import Fernet, InvalidToken
from . import group  # Import the group module for data access


# Some exceptions we may face
class DBNotFoundError(Exception):
    # This exception is raised when you try to open a database that has not been created yet
    pass


class DBExistsError(Exception):
    # This exception is raised when you try to create a database that already exists
    pass


class DecryptionFailedError(Exception):
    # This exception is raised when the module fails to decrypt a metadata file.
    # This generally happens when you manually manipulate the encrypted metadata
    # file or if you make changes to the encryption key
    pass


def create(name: str, dbPath: str = ".", safeMode: bool = True):
    """
    This method creates a database by the name of 'name'.
    If safeMode is not set to True, it raises DBExistsError
    if the requested database already exists.

    @param name <str> [Required]: Name of the db.
    @param safeMode <bool> [Optional]: Whether to use safeMode.
                                       Enabled by default
    @param dbPath <str> [Optional]: The relative path the directory
                                    where the 'db' directory is located.
                                    Default: Current directory.

    @returns: utils.database.db.db instance of the newly created
              database
    """

    # Be sure of the data types of the params
    name, safeMode, dbPath = str(name), bool(safeMode), str(dbPath)

    # Check if the given path is correct
    if "db" not in os.listdir(dbPath):
        raise ValueError(
            f"The given path {dbPath}, is not a\
             valid database directory.\n(Could \
            not find `db` subdirectory in `{dbPath}`)"
        )

    # Check if the name is correct
    if re.match(r".*[+/\\,.^%!@#$&*(){}\[\]'\"<>\?\|= ].*", name):
        raise ValueError(
            f"The name `{name}` is not a vaid name\
             for a database.\nA valid name is one \
            with just alphanumeric characters(0-9, \
            a-z, A-Z), hyphens(-) and underscores(_)"
        )

    if name in os.listdir(f"{dbPath}/db"):  # If the database already exists

        if not safeMode:  # And safe mode is disabled

            # Raise an error
            raise DBExistsError(f"The requested database '{name}' already exists.")
        else:

            # Or if safeMode is enabled give them the database
            return db(name, dbPath=dbPath, safeMode=safeMode)

    # Otherwise create the database
    else:

        # Create a directory for our new db
        os.mkdir(f"{dbPath}/db/{name}")

        # Add some metadata to it for faster group-access
        with open(f"{dbPath}/db/{name}/metadata.json", "x") as mdata:
            key = Fernet.generate_key().decode("utf-8")
            mdata.write(
                json.dumps(
                    {
                        "name": name,  # Name of the db
                        "groups": [],  # New databases are empty
                        "key": key,  # The encryption key for this db
                    }
                )
            )

        # Return the newly created database
        return db(name, dbPath=dbPath, safeMode=safeMode)


# Function to get all the databases
def getAllDbs(dbPath="."):
    """
    This function returns a list containing the names of all the databases in
    the /db subdirectory of the directory 'dbPath'
    """

    # Check if the given path is correct
    if "db" not in os.listdir(dbPath):
        raise ValueError(
            f"The given path {dbPath}, is not a valid \
            database directory.\n(Could not find `db` \
            subdirectory in `{dbPath}`)"
        )

    return os.listdir(dbPath + "/db")


# The main thing comes here, the db class
class db:
    """
    Get a database instance.

    @param name <str>: Name of the database.\n
    @param safeMode <bool> [Opional]: Whether to get groups in safeMode or not. Defaults to True.\n
    @param preLoad <bool>  [Optional]: Whether to preload group names for faster group access. Defaults to True.\n
    @param dbPath <str> [Optional]: The path to the directory where the 'db' directory is located.
                                    Default: Current directory
    """

    # Initiation of our database
    def __init__(
        self, name: str, dbPath: str = ".", safeMode: bool = True, preLoad: bool = True
    ):

        # Store the dbPath
        self.dbPath = str(dbPath)

        # Check if the given path is correct
        if "db" not in os.listdir(self.dbPath):
            raise ValueError(
                f"The given path {self.dbPath}, is not a valid database directory. \
                \n(Could not find `db` subdirectory in `{self.dbPath}`"
            )

        # Check if the name is correct
        if re.match(r".*[+/\\\\,.^%!@#$&*(){}\[\]'\"<>\?\|= ].*", name):
            raise ValueError(
                f"The name `{name}` is not a vaid name for\
                 a database.\nA valid name is one with just\
                 alphanumeric characters(0-9, a-z, A-Z), \
                hyphens(-) and underscores(_)"
            )

        # Check if the asked database is availabe:
        if name not in os.listdir(f"{self.dbPath}/db/"):

            # Create the db if safeMode is enabled
            if safeMode:
                create(name, dbPath=dbPath)

            # Otherwise display an error
            else:
                raise DBNotFoundError(
                    f"The requested db {name} has not been\
                    created yet. Either create it using create\
                    method or enable safeMode."
                )

        # Save the data otherwise
        self.name = name
        self.safeMode = safeMode
        self.key = None  # These are just there till we
        self.groups = []  # load the metadata.

        # Get the preload data if it is enabled
        if preLoad:
            self.get_meta()

    def __getitem__(self, name):
        return self.getGroup(name)

    # This function loads the metadata about the database. For internal
    # use only. Not to be used by users.
    def get_meta(self):

        # Open the metadata file
        with open(f"{self.dbPath}/db/{self.name}/metadata.json", "r") as mdata:

            # Load the data
            d = json.loads(mdata.read())

            # Save the data
            self.key = d["key"]
            self.groups = d["groups"]

    # Function to drop this database
    def drop(self):
        """
        Delete this database.
        Warning: This method deletes ALL the data stored inside it and it is NOT reversible.
        """

        # Delete the database folder
        shutil.rmtree(f"{self.dbPath}/db/{self.name}")
        # And we're done!

    # Function to create a new group
    def createGroup(self, name: str, safeMode: bool = True, preLoad: bool = False):
        """
        Create a new data group in the current database.

        Raises GroupExistsError if group already exists and database is not in safe mode.
        @param name <str>: Name of the new group.
        @param safeMode <bool> [Optional]: Whether to use safeMode or not
        @param preLoad <bool> [Optional]: Whether to preLoad data or not

        @returns utils.database.group.group instance of the new group.
        """

        # Check if the name is correct
        if re.match(r".*[+/\\\\,.^%!@#$&*(){}\[\]'\"<>\?\|= ].*", name):
            raise ValueError(
                f"The name `{name}` is not a vaid name\
                for a database.\nA valid name is one wi\
                th just alphanumeric characters(0-9, a-z\
                , A-Z), hyphens(-) and underscores(_)"
            )

        # If the group already exists
        if name + ".group" in os.listdir(f"{self.dbPath}/db/{self.name}"):

            # If the safeMode is enabled
            if safeMode:

                # Return the normal instance
                return self.getGroup(name, preLoad=preLoad)

            # Otherwise raise an error
            else:
                raise group.GroupExistsError(f"The group {name} already exists.")

        # Create it if its not there
        else:

            # If key is not there then get it
            if self.key is None:
                self.get_meta()

            # Create our file
            with open(f"{self.dbPath}/db/{self.name}/{name}.group", "x") as grp:

                # Write an empty list into it for now
                grp.write(Fernet(self.key).encrypt(b"[]").decode("utf-8"))

            # Add the group to our in-memory list of groups
            self.groups.append(name)

            # Now update our metadata file
            f = open(f"{self.dbPath}/db/{self.name}/metadata.json", "w")
            f.write(
                json.dumps({"name": self.name, "key": self.key, "groups": self.groups})
            )

            return self.getGroup(name, safeMode=safeMode, preLoad=preLoad)

    # This function is used to access a group
    def getGroup(self, name: str, safeMode: bool = True, preLoad: bool = False):
        """
        Access a data group in the database.
        Raises GroupNotFoundError if the group is not found and safeMode is disabled. If safeMode is enabled, it creates the group.

        @param name <str>: Name of the group you want to access.
        @param safeMode <bool> [Optional]: Whether to open the group in safeMode or not. Default: True.
        @param preLoad <bool> [Optional]: Whether to pre-load the group for faster data access.

        WARNING: Setting pre-load to True makes a copy of the data in the memory (RAM) of the machine. So, using this setting is not recommended for large groups or if you are low on memory.

        @returns utils.database.group.group instance of the group.
        """

        # If the group does not exist
        if name not in self.groups:

            # If safe mode is enabled
            if safeMode:

                # Create the requested group
                return self.createGroup(name, preLoad=preLoad)

            # Otherwise
            else:

                # Throw an error that it does not exist
                raise group.GroupNotFoundError(
                    f"The requested group {name} has no been created."
                )

        # The group exists
        else:

            # Get the user the requested group
            return group.group(name, preLoad=preLoad, parent=self)

    # Function to export our database for sharing
    def export(self, path):
        """
        This function packages the current database into a single file and saves the
        file in the path provided as the param. This function can be used to create
        encrypted backups for the database.

        @param path <path>: This should be a relative or absolute path to the directory
        where the output package should be saved.

        @returns key: The output file is encrypted. To use it again you should provide
        the key returned by this function
        """

        # Get the absolute path of the required directory
        path = os.path.abspath(path)

        # If the given path is not a valid path
        if not os.path.isdir(path):

            # Raise a value error
            raise ValueError("The provided path %s is invalid" % path)

        # If the given path is not writable
        if not os.access(path, os.W_OK):

            # Raise a value error
            raise ValueError("The provided path %s is not writable" % path)

        # A skeleton json for the export package
        d = {
            "name": self.name,  # Name
            "key": self.key,  # Encryption key of the groups
            "groups": [],  # The groups
        }

        # Loop through all the groups
        for i in self.groups:

            # Open the group
            with open(f"{self.dbPath}/db/{self.name}/{i}.group", "r") as f:

                # Add the group data to our export package
                d["groups"].append({"name": i, "data": f.read().encode()})

        # Generate a key for export package
        out_key = Fernet.generate_key()

        # Generate and then write to the output file
        with open(f"{path}/{self.name}.amazedb", "x") as out:

            # Encrypt the data before writing
            out.write(Fernet(out_key).encrypt(json.dumps(d).encode()).decode("utf-8"))

        # Return the encryption key
        return out_key

    # Function to import data from a package
    def import_data(self, path, key):
        """
        Function to import data to this database from an export package generated
        by the util.database.db.db.export metod.

        WARNING: This function will delete all the groups (and thus data) in the database.

        @param path <path>: This should be a valid path to the file which needs to be imported.
        @param key <bytes>: This should be a vaid encryption/decryption key of the file
        """

        # Handle some exceptions:
        try:

            # Open the file
            with open(path, "r") as f:

                # Try to read the file
                d = f.read().encode("utf-8")

                # Decrypt the file
                d = Fernet(key).decrypt(d)

                # Load the data
                d = json.loads(d)

                # Delete all existing groups
                [group.group(self, i).drop() for i in self.groups]
                self.groups = []

                # Create new groups
                for i in d["groups"]:

                    # Create a new file
                    gr = open(f'{self.dbPath}/db/{self.name}/{i["name"]}.group', "x")

                    # Save the data
                    gr.write(i["data"])

                    # close
                    gr.close()

                    # Update the self.groups list
                    self.groups.append(i["name"])

                # Edit the current encryption key
                self.key = d["key"]

                # Update our metadata
                md = open(f"{self.dbPath}/db/{self.name}/metadata.json", "w")
                md.write(json.dumps({"name": self.name, "groups": self.groups}))
                md.close()

        # The provided file was not found
        except FileNotFoundError:
            raise FileNotFoundError("The provided file %s was not found" % path)

        # The provided key is not the correct key
        except InvalidToken:
            raise ValueError("The provided key %s is not a vaild key" % key)

        # Some other exception
        except Exception as exc:
            raise Exception(
                "Encountered a problem while importing {}: \n\t\t{}".format(path, exc)
            )
