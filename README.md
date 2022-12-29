# amazedb

![Tests](https://github.com/jalaj-k/amazedb/workflows/Tests/badge.svg)
![Upload Python Package](https://github.com/jalaj-k/amazedb/workflows/Upload%20Python%20Package/badge.svg)
![Version](https://img.shields.io/badge/Version-1.1.0-blue)
![License](https://img.shields.io/badge/License-MIT-yellow)

 It is a file based NoSQL database management system written in python.
 
 All the databases are stored in the `db` sub-directory of the current directory. This behaviour can be manipulated as we'll see later on.

## How to use

You can install amazedb through pip.

    $ python -m pip install amazedb

Example usage

```python
from amazedb import dbms

# Open a database
mydb = dbms.db("mydatabase")

# Create a group inside it
users = mydb.createGroup("users")

# Or access it via
users = mydb.getGroup("users")

# Add some data to our users
users.insert({
    "name": "Jalaj",
    "role": "admin",
    "email": "example@example.org",
    "age": 17
})

# Or add multiple documents simultaneously
users.insert_many(
    {"name": "ABCD", "age": 10},
    {"name": "EFGH", "age": 20},
    {"name": "IJKL", "age": 30}
)

# Get the user with the name ABCD
abcd = user.get_one({"name": "ABCD"})

# Or use some advanced search filters
abcd = users.get_one({
    "name": {"__re" : ".*A.*"} # All users with A in their name,
    "age": {"__gt": 10} # All users with age more than 10
})

# Or get them sorted
# Get all users sorted by age
abcd = users.get({}, sortby="age")

# Update some values
updated = users.update({
    "name" : "ABCD"         # Find the user with the name ABCD
}, {
    "email" : "abcd@example.org" # And update its email field
})

# Use advanced search in update function
updated = users.update({
    # All users with age greater than or equal to 10
    "age": {"__gte" : 10}
}, {
    "age": 5
})

# Update the firs occurence of search filter
update_one = users.update_one({

    # You can also use custom functions for search
    "name": { "__cf": lambda val: True if len(val) > 10 else False}
}, {
    "name": "newName"
})

# Delete some documents
delete = users.remove({
    "name": "ABCD"
})

# Delete only one document
del_one = users.remove_one({
    "name": "EFGH"
})

# Delete the group
users.drop()

# Delete the database
mydb.drop()

```

For detailed usage instructions, refer to [USAGE.md](USAGE.md)

## Contributing

Contributions to our project through new [issues](https://github.com/jalaj-k/amazedb/issues/new/choose) and [pull requests](https://github.com/jalaj-k/amazedb/pulls) are always welcome.
We would love to see other people contribute to the project and make it better.

Although, before you contribute, you should follow our [issue templates](.github/ISSUE_TEMPLATE). 
Any PR or issue not following one of the templates will be ignored and closed immediately. 
And yes, before opening a issue and don't forget to follow the [code of conduct](CODE_OF_CONDUCT.md)

## License

This project is licensed under [MIT](LICENSE).

