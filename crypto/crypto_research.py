import time
import re
import aiohttp
import asyncio
import json
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import json
def get_orgs_name():
    with open('org_names.txt', 'r+') as f:
        org_names = json.load(f)
    for i in range(1, 11):
        page_url = f'https://coinmarketcap.com/?page={i}'
        response = requests.get(page_url)
        soup = BeautifulSoup(response.text, 'lxml')

        # Find the GitHub link
        coin_links = soup.find_all('a', href=lambda href: href and '/currencies/' in href and '/#markets' not in href)
        for counter, link in enumerate(coin_links):
            print(f"{counter}/{len(coin_links)}")
            coin_page = link['href']
            coin_name = coin_page.replace('/currencies/', '').replace('/', '').lower()
            if coin_name in org_names:
                continue
            url = f'https://coinmarketcap.com{coin_page}'
            try:
                response = requests.get(url)
            except Exception as e:
                print(e)
                continue
            soup = BeautifulSoup(response.text, 'lxml')

            # Find the GitHub link
            github_link = soup.find('a', href=lambda href: href and 'github.com/' in href)
            if github_link:
                main_repo_url = github_link['href'].replace('//github.com/orgs/', '').replace('//github.com/', '').replace('//www.github.com/')
                main_repo_url = re.sub(r'/$', '', main_repo_url)
                github_org_name = main_repo_url.split('/')[0]
                if not github_org_name:
                    github_org_name = github_link['href'].replace('//www.github.com/', '')
                if github_org_name:
                    org_names[coin_name] = {'org_name': github_org_name, 'main_repo': main_repo_url}
    with open('org_names.txt', 'w') as f:
        json.dump(org_names, f)



orig_tokens = ['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',
                        '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',
                        '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',
                        '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',
                        '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',
               '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',
               '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'
               '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',
               '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',
               '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'
               ]

async def fetch(session, url, api, tokens):
    for i in range(len(orig_tokens)):
        headers = {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {api}"
        }
        async with session.get(url, headers=headers, ssl=False, allow_redirects=False) as response:
            if response.status == 403:
                print(f"Error {response.status} for {url}")
                if tokens:
                    print('Меняю токен')
                    api = tokens.pop(0)
                else:
                    print('обновляю токены')
                    time.sleep(60)
                    tokens = orig_tokens.copy()
                    api = tokens.pop(0)
            elif response.status == 404:
                print(f"Error {response.status} for {url}")
                return True
            elif response.status in [301, 302]:
                return response.headers.get('Location')
            elif response.status != 200:
                print(f"Error {response.status} for {url}")
                return None
            else:
                try:
                    return await response.json(), api, tokens
                except:
                    return

async def get_num_commits_in_all_repos(repo_list, api, tokens, session, date, other_langs_counter=0, typescript_counter=0):
    for counter, repo in enumerate(repo_list):
        try:
            repo_url = repo['url']
        except:
            continue
        if not repo.get('description', '') or repo['language'] is None:
            continue
        if ('frontend' in repo['description'] or 'front-end' in repo['description']) and repo['language'].lower() in ['ruby', 'typescript']:
            continue
        check_lst = ['markdown', 'mdx', 'html', 'shell',
                    'kotlin', 'swift', 'dockerfile',
                    'javascript', 'css', 'json', 'wikitext']
        if repo['language'].lower() in check_lst:
            continue

        result = await run_query(session, repo, api, tokens)
        issues = result['data']['repository']['issues']['totalCount']
        pulls = result['data']['repository']['pullRequests']['totalCount']
        i = 1
        branches = True
        while branches:
            branches_url = f"{repo_url}/branches?per_page=100&page={i}"
            async with aiohttp.ClientSession() as session:
                answer = await fetch(session, branches_url, api, tokens)
                if answer is None:
                    break
                branches, api, tokens = answer
                if branches:
                    i +=1
                for branch_counter, branch in enumerate(branches):
                    try:
                        branch_name = branch['name']
                    except:
                        pass
                        continue

                    commits_url = f"{repo_url}/commits?sha={branch_name}&since={date}"
                    answer = await fetch(session, commits_url, api, tokens)
                    if answer is None:
                        break
                    if answer is True:
                        break
                    commits, api, tokens = answer
                    if commits:
                        if 'typescript' == repo['language'].lower():
                            typescript_counter += len(commits)
                        other_langs_counter += len(commits)
    return other_langs_counter, typescript_counter, issues, pulls

with open('org_names.txt', 'r+') as f:
    coin_names_repo_names = json.load(f)

async def main(file_name):
    tokens = orig_tokens.copy()
    api = tokens.pop(0)
    with open(file_name + '.txt', 'r+') as f:
        tokens_data = json.load(f)
    async with aiohttp.ClientSession() as session:
        for counter, coin_name in enumerate(coin_names_repo_names):
            c = 0
            typescript_counter = 0
            org_name = coin_names_repo_names[coin_name]['org_name']
            if org_name in tokens_data:
                continue
            main_repo = coin_names_repo_names[coin_name]['main_repo'].replace('//www.github.com/', '')
            main_repo = re.sub(r'/$', '', main_repo)

            if len(main_repo.split('/')) > 1:
                url = f'https://github.com/{main_repo}'
                answer = await fetch(session, url, api, tokens)
                if answer is not None:
                    try:
                        org_name = answer.split('/')[3]
                        if org_name in tokens_data:
                            continue
                    except:
                        continue
            print(f'Coin name: {coin_name}, {counter}/{len(coin_names_repo_names)}')
            #getting repos
            foo, all_repos, flag = 1, [], True
            while flag:
                url = f"https://api.github.com/users/{org_name}/repos?sort=pushed&per_page=100&page={foo}"
                answer = await fetch(session, url, api, tokens)
                if answer is None:
                    break
                elif answer is True:
                    break
                repo_list, api, tokens = answer
                if not repo_list or type(repo_list) == dict:
                    break
                for repo in repo_list:
                    try:
                        commit_date = datetime.strptime(repo['pushed_at'], "%Y-%m-%dT%H:%M:%SZ")
                    except:
                        pass
                    today = datetime.utcnow()  # Используйте UTC, чтобы корректно сравнить даты в одном формате
                    final_date = today - timedelta(days=60)
                    if commit_date < final_date:
                        flag = False
                        break
                    all_repos.append(repo)
                foo +=1
            if all_repos:
                other_langs_counter, typescript_counter, issues, pulls = await get_num_commits_in_all_repos(repo_list=all_repos, api=api, tokens=tokens, session=session, date=final_date)
            tokens_data.setdefault(org_name, {}).setdefault('repo_counter_others', []).append(other_langs_counter)
            tokens_data[org_name]['issues'] = issues
            tokens_data[org_name]['pulls'] = pulls
            tokens_data[org_name]['repo_counter_no_typescript'] = other_langs_counter+typescript_counter

            with open(file_name + '.txt', 'r+') as f:
                sorted_commits_data = dict(sorted(tokens_data.items(), key=lambda item: item[1]["repo_counter_no_typescript"][0], reverse=True))
                json.dump(sorted_commits_data, f)

async def run_query(session, repo, api, tokens):
    today = datetime.utcnow()
    final_date = (today - timedelta(days=365)).isoformat() + "Z"
    variables = {
        "owner": repo['owner']['login'],
        "name": repo['name'],
        "pageSize": 100,  # Изменено с 101 на 100, т.к. GitHub ограничивает до 100
        "issuesAfter": None,
        "pullsAfter": None,
        "since": final_date
    }

    query = """
    query(
      $owner: String!,
      $name: String!,
      $issuesAfter: String,
      $pullsAfter: String,
      $since: DateTime!
    ) {
      repository(owner: $owner, name: $name) {
        issues(
          first: 100,
          after: $issuesAfter,
          orderBy: {field: CREATED_AT, direction: DESC},
          filterBy: {since: $since},
          states: [OPEN, CLOSED]
        ) {
          totalCount
          edges {
            node {
              number
              title
              state
              createdAt
              closedAt
            }
            cursor
          }
          pageInfo {
            endCursor
            hasNextPage
          }
        }
        pullRequests(
          first: 100,
          after: $pullsAfter,
          orderBy: {field: CREATED_AT, direction: DESC},
          states: [OPEN, MERGED]
        ) {
          totalCount
          edges {
            node {
              number
              title
              state
              createdAt
              mergedAt
            }
            cursor
          }
          pageInfo {
            endCursor
            hasNextPage
          }
        }
      }
    }
    """
    headers = {
        "Authorization": f"Bearer {api}",
        "Content-Type": "application/json"
    }
    async with session.post("https://api.github.com/graphql", json={"query": query, "variables": variables},
                            headers=headers, ssl=False) as response:
        if response.status != 200:
            text = await response.text()
            raise Exception(f"Query failed with status {response.status}: {text}")
        return await response.json()

if __name__ == '__main__':
    # get_orgs_name()
    asyncio.run(main('tokens_data'))