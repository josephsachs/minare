try {
    // Wait for MongoDB to be ready
    sleep(1000);

    // Configuration with explicit localhost binding
    const config = {
        _id: "rs0",
        members: [
            {
                _id: 0,
                host: "localhost:27017",
                priority: 1
            }
        ]
    };

    // Initialize replica set
    rs.initiate(config);

    // Wait for primary election
    sleep(2000);

    // Switch to admin database
    db = db.getSiblingDB('admin');

    // Create admin user if doesn't exist
    if (!db.getUser("admin")) {
        db.createUser({
            user: "admin",
            pwd: "example",
            roles: ["root"]
        });
    }

    print("Replica set initialization complete");
} catch (error) {
    print("Error during initialization:");
    print(error);
}