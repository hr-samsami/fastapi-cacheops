import hashlib
import redis
from sqlalchemy import create_engine, select, event
from sqlalchemy.orm import declarative_base, Session
from sqlalchemy import Column, Integer, String
import json
import pickle

# Set up Redis and SQLAlchemy
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)
engine = create_engine("sqlite:///:memory:", echo=True)

Base = declarative_base()

class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    name = Column(String)

    def __repr__(self):
        # Customize how the User instance is printed (only id and name)
        return f"User(id={self.id}, name={self.name})"

Base.metadata.create_all(engine)

# Insert sample data
with Session(engine) as session:
    session.add_all([User(name="Alice"), User(name="Bob"), User(name="Charlie")])
    session.commit()

# Helper function to get a cache key for a query
def get_cache_key(clauseelement, params):
    # Create a unique cache key by hashing the SQL query and params
    query_str = str(clauseelement) + json.dumps(params, sort_keys=True)
    return hashlib.md5(query_str.encode('utf-8')).hexdigest()

# Define the intercept function
def before_execute(conn, clauseelement, multiparams, params, execution_options):
    print("Before executing SELECT query:")
    # Print the actual SQL query
    print(clauseelement)
    
    # Check and print parameters for debugging
    if params:
        print(f"Query parameters: {params}")
    elif multiparams:
        print(f"Multiple parameters: {multiparams}")
    print("before_execute triggered")
    if not str(clauseelement).lower().startswith("select"):
        # Only cache SELECT queries
        print("Non-SELECT query detected, skipping caching.")
        return clauseelement, multiparams, params

    # Generate a cache key for the query
    cache_key = get_cache_key(clauseelement, params)
    
    # Check if the result is cached in Redis
    cached_result = redis_client.get(cache_key)
    if cached_result:
        print("Returning data from Redis cache.")
        return cached_result  # Stop execution; return cached data as the result
    
    print("No cached result found; proceeding to database.")
    return clauseelement, multiparams, params

# Attach the listener to intercept SELECT queries
event.listen(engine, "before_execute", before_execute)

# Utility function to convert a SQLAlchemy model instance to a dictionary
def row_to_dict(row):
    return {key: value for key, value in row._mapping.items()}

# Run a query to see caching in action
with Session(engine) as session:
    stmt = select(User).where(User.name == "Bob")
    print(str(stmt.compile(dialect=engine.dialect,compile_kwargs={"literal_binds": True})))
    print(f"Executing query: {str(stmt)}")  # Print the query string for debugging
    result = session.execute(stmt)
    
    # If data came from Redis, it will already be available. Otherwise, cache it.
    cache_key = get_cache_key(stmt, {})
    if not redis_client.get(cache_key):
        # Fetch all rows and convert each row to a dictionary automatically
        rows = result.all()  # Fetch all rows as a list of Row objects

        # Convert each row into a dictionary using row_to_dict for any model
        data_to_cache = [row_to_dict(row) for row in rows]
        serialized = pickle.dumps(data_to_cache)
        # Store the JSON representation of rows in Redis
        redis_client.set(cache_key, serialized)
        print("Data cached in Redis.")
    else:
        # Data was fetched from Redis, already handled in `before_execute`
        print("Data was fetched from Redis cache.")

    # Print the result
    print("Query result:")
    for row in result:
        print(row)
