from typing import Dict, List
from langchain.tools import BaseTool

from agents.base_agents import CollectionRouterAgent
from models.agent_models import PreprocessedQuery, CollectionReference
from tools.database_tools import ListCollectionsTool
from models.schema import FieldType, FieldDefinition


class CollectionRouter(CollectionRouterAgent):
    """Implementation of the collection routing agent"""

    def __init__(self, db):
        super().__init__()
        self.tools = [ListCollectionsTool(db=db)]

    async def process(self, preprocessed_query: PreprocessedQuery, available_collections: List[Dict]) -> CollectionReference:
        """Identify the relevant collection for a query"""

        try:
            # Analyze the query and collections to determine the best match
            collection_analysis = await self.llm.ainvoke(
                f"""Given this normalized query and available collections, determine:
                1. Which collection is most relevant
                2. Whether a new collection needs to be created
                3. Your confidence in the match (0.0 to 1.0)

                Query: "{preprocessed_query.normalized_query}"

                Available Collections:
                {available_collections}

                Consider:
                - Collection names and descriptions
                - Schema fields and their purposes
                - The intent of the query
                - Whether existing collections can handle the data

                Format your response as a JSON object with:
                - collection_name: str
                - confidence_score: float
                - create_new: bool
                - schema: dict (if create_new is true)
                - description: str (if create_new is true)"""
            )

            if not isinstance(collection_analysis, dict):
                raise ValueError("Invalid collection analysis response")

            # If creating a new collection
            if collection_analysis.get("create_new", False):
                # Generate a schema based on the query
                schema_analysis = await self.llm.ainvoke(
                    f"""Create a schema for a new collection based on this query:
                    "{preprocessed_query.normalized_query}"

                    The schema should include appropriate fields for storing and querying the data.
                    Consider what fields would be needed for future queries and operations.

                    Available field types:
                    - STRING
                    - INTEGER
                    - FLOAT
                    - BOOLEAN
                    - DATETIME
                    - SELECT (single option)
                    - MULTI_SELECT (multiple options)

                    Format your response as a JSON object with:
                    - fields: list of field definitions
                      Each field should have:
                      - name: str
                      - description: str
                      - field_type: str (one of the above types)
                      - required: bool
                      - default: any (optional)
                      - options: list[str] (for SELECT/MULTI_SELECT only)"""
                )

                if not isinstance(schema_analysis, dict):
                    raise ValueError("Invalid schema analysis response")

                # Convert the schema analysis to field definitions
                fields = []
                for field in schema_analysis["fields"]:
                    fields.append(
                        FieldDefinition(
                            name=field["name"],
                            description=field["description"],
                            field_type=FieldType(field["field_type"]),
                            required=field.get("required", False),
                            default=field.get("default"),
                            options=field.get("options"),
                        )
                    )

                return CollectionReference(
                    collection_name=collection_analysis["collection_name"],
                    confidence_score=1.0,  # High confidence for new collections
                    schema={"fields": fields},
                    create_new=True,
                )

            # If using existing collection
            matching_collection = next((c for c in available_collections if c["name"] == collection_analysis["collection_name"]), None)

            if not matching_collection:
                raise ValueError(f"Collection {collection_analysis['collection_name']} not found")

            return CollectionReference(
                collection_name=matching_collection["name"],
                confidence_score=collection_analysis["confidence_score"],
                schema=matching_collection["schema"],
                create_new=False,
            )

        except Exception as e:
            # If we can't determine the collection, suggest creating a new one
            # with a generic schema based on the query
            fallback_analysis = await self.llm.ainvoke(
                f"""The query could not be matched to an existing collection.
                Analyze this query and suggest an appropriate collection structure:
                
                Query: "{preprocessed_query.normalized_query}"
                
                Create a collection schema that would be suitable for this type of data.
                Format your response as a JSON object with:
                - collection_name: str (a suitable name for this data)
                - fields: list of field definitions
                  Each field should have:
                  - name: str
                  - description: str
                  - field_type: str (STRING, INTEGER, FLOAT, BOOLEAN, DATETIME, SELECT, MULTI_SELECT)
                  - required: bool
                  - default: any (optional)
                  - options: list[str] (for SELECT/MULTI_SELECT only)
                - description: str (purpose of this collection)"""
            )

            if not isinstance(fallback_analysis, dict):
                raise ValueError("Invalid fallback analysis response")

            # Convert the fallback analysis to field definitions
            fields = []
            for field in fallback_analysis["fields"]:
                fields.append(
                    FieldDefinition(
                        name=field["name"],
                        description=field["description"],
                        field_type=FieldType(field["field_type"]),
                        required=field.get("required", False),
                        default=field.get("default"),
                        options=field.get("options"),
                    )
                )

            return CollectionReference(
                collection_name=fallback_analysis["collection_name"],
                confidence_score=0.0,  # Low confidence since this is a fallback
                schema={"fields": fields},
                create_new=True,
            )
