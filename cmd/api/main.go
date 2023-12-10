package main

func main() {
	// TODO: [Simas] Add run logic here
	// TODO: [Simas] There aren’t any tests.
	// TODO: [Simas] There’s no Close method to gracefully close the file.
	// TODO: [Simas] The service can close with events still in the write buffer: events can get lost.
	// TODO: [Simas] Keys and values aren’t encoded in the transaction log: multiple lines or white‐ space will fail to parse correctly.
	// TODO: [Simas] The sizes of keys and values are unbound: huge keys or values can be added, fill‐ ing the disk.
	// TODO: [Simas] The transaction log is written in plain text: it will take up more disk space than it probably needs to.
	// TODO: [Simas] The log retains records of deleted values forever: it will grow indefinitely.
}
