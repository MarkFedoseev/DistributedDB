class Database:
    def __init__(self):
        self.data = {}

    def create(self, key, value):
        if key not in self.data:
            self.data[key] = value
            return f"Record {key} created."
        else:
            return "Error: Key already exists."

    def read(self, key):
        if key in self.data:
            return self.data[key]
        else:
            return "Error: Key not found."

    def update(self, key, value):
        if key in self.data:
            self.data[key] = value
            return f"Record {key} updated."
        else:
            return "Error: Key not found."

    def delete(self, key):
        if key in self.data:
            del self.data[key]
            return f"Record {key} deleted."
        else:
            return "Error: Key not found."


if __name__ == '__main__':
    # Usage
    db = Database()
    db.create('name', 'John Doe')
    print(db.read('name'))
    db.update('name', 'Jane Doe')
    print(db.read('name'))
    db.delete('name')
    print(db.read('name'))  # should print 'Error: Key not found.'