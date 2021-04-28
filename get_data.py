import dataclasses
import os
import typing

import pandas as pd
import requests
from rich.progress import track


@dataclasses.dataclass
class GitHubGraphQLQuery:
    BASE_URL: typing.ClassVar[str] = "https://api.github.com/graphql"
    github_token: str = dataclasses.field(default=None, repr=False)
    query: str = None
    query_variables: typing.Dict[str, str] = None
    additional_headers: typing.Dict[str, str] = None

    def __post_init__(self):
        if self.additional_headers is None:
            self.additional_headers = {}

        if self.query_variables is None:
            self.query_variables = {}
        default_headers = dict(
            Authorization=f"token {self.github_token}",
        )
        self.headers = {**default_headers, **self.additional_headers}

    @property
    def data(self):
        ...

    def generator(self):
        while True:
            yield self.run()

    def iterator(self):
        ...

    def run(self):
        try:
            return requests.post(
                GitHubGraphQLQuery.BASE_URL,
                headers=self.headers,
                json=dict(query=self.query, variables=self.query_variables),
            ).json()

        except requests.exceptions.HTTPError as http_err:
            raise http_err

        except Exception as err:
            raise err


@dataclasses.dataclass
class UserContributions(GitHubGraphQLQuery):
    @property
    def data(self):
        results = self.run()["data"]
        user = results["user"]["login"]
        acccountCreationDate = results["user"]["login"]
        weeks = results["user"]["contributionsCollection"]["contributionCalendar"][
            "weeks"
        ]
        contributions = []
        for week in weeks:
            contributions.extend(week["contributionDays"])
        data = pd.DataFrame(contributions)
        data["user"] = user
        return data


@dataclasses.dataclass
class Members(GitHubGraphQLQuery):
    @property
    def data(self):
        return pd.DataFrame(self.iterator())

    def iterator(self):
        generator = self.generator()
        has_next_page = True
        data = []
        while has_next_page:
            response = next(generator)
            page_info = response["data"]["organization"]["membersWithRole"]["pageInfo"]
            end_cursor = page_info["endCursor"]
            has_next_page = page_info["hasNextPage"]
            self.query_variables["after"] = end_cursor
            users = response["data"]["organization"]["membersWithRole"]["edges"]
            data.extend(users)

        return [user["node"] for user in data]


if __name__ == "__main__":
    token = os.environ["GH_PERSONAL_TOKEN"]
    members_query = """query ($org: String!, $after: String) {
  organization(login: $org) {
    membersWithRole(first: 100, after: $after) {
      edges {
        node {
          login
          name
        }

      }
      pageInfo {
        hasNextPage
        endCursor
      }
    }
  }
}"""
    user_contributions_query = """query($user:String!, $since:DateTime!, $until:DateTime!){
 user(login: $user) {
    login
    createdAt
    contributionsCollection(from: $since, to: $until) {
      contributionCalendar {
        totalContributions
        weeks {
          contributionDays {
            date
            contributionCount
          }
        }
      }
    }
}
}
"""

    members = Members(token, members_query, {"org": "NCAR", "after": None}).data
    usernames = members.login.tolist()

    contributions = []
    years = [
        (f"{year}-01-01T00:00:00Z", f"{year}-12-31T23:59:59Z")
        for year in range(2008, 2022)
    ]
    for user in track(usernames, description="Harvesting data...."):
        for entry in years:
            variables = {"user": user, "since": entry[0], "until": entry[1]}
            contributions.append(
                UserContributions(token, user_contributions_query, variables).data
            )
    df = pd.concat(contributions)
    df.to_csv("ncar-members-contributions.csv.gz", index=False)
