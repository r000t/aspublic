import argparse
import webbrowser
from mastodon import Mastodon

parser = argparse.ArgumentParser(description="Pops a browser tab to an instance's OAuth page to obtain an access token")
parser.add_argument('domain', type=str, help="Instance domain")
parser.add_argument('--name', type=str, default="as:Public Collector", help="App name to present to instance")

args = parser.parse_args()

scope = ['read:statuses']

client_id, client_secret = Mastodon.create_app(args.name, api_base_url=args.domain, scopes=scope)
mpy = Mastodon(api_base_url=args.domain,  client_id=client_id, client_secret=client_secret)
webbrowser.open_new_tab(mpy.auth_request_url(scopes=scope))

print("A log-in/authorization page was opened in your default browser.")
authcode = input("Paste the code you receive, here: ")

token = mpy.log_in(code=authcode, scopes=scope)

print(f"Your access token is {token}")
print("For use with the collector, you can add this line to an instance list:")
print(f"{args.domain}|{token}")