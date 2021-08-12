from collections import defaultdict

# Download whatever files you want to include in analysis and update the paths here:
input_file_names = [
    "D:\\Downloads\\17lands\\game_data_public.AFR.PremierDraft.csv",
    "D:\\Downloads\\17lands\\game_data_public.AFR.TradDraft.csv",
    "D:\\Downloads\\17lands\\game_data_public.STX.PremierDraft.csv",
    "D:\\Downloads\\17lands\\game_data_public.STX.TradDraft.csv",
    "D:\\Downloads\\17lands\\game_data_public.KHM.PremierDraft.csv",
    "D:\\Downloads\\17lands\\game_data_public.KHM.TradDraft.csv",
]
gems = {
    "TradDraft": {
        3: 3000,
        2: 1000,
        1: 0,
        0: 0,
    },
    "PremierDraft": {
        7: 2200,
        6: 1800,
        5: 1600,
        4: 1400,
        3: 1000,
        2: 250,
        1: 100,
        0: 50,
    }
}
packs = {
    "TradDraft": {
        3: 6,
        2: 4,
        1: 1,
        0: 1,
    },
    "PremierDraft": {
        7: 6,
        6: 5,
        5: 4,
        4: 3,
        3: 2,
        2: 2,
        1: 1,
        0: 1,
    }
}
min_matches = {
    "PremierDraft": 50,
    "TradDraft": 10,
}


def main():
    matches = []
    for input_file_name in input_file_names:
        print("collecting matches from " + input_file_name + "...")
        matches.extend(get_matches(input_file_name))
    print("processing matches...")
    drafts = get_drafts(matches)
    buckets = get_buckets(drafts)
    processed_buckets = process_buckets(buckets)
    for event_type in sorted(processed_buckets):
        print(event_type + " text data input:")
        print("win_rate_in_bucket,average_gems,average_packs,draft_count")
        for bwr in sorted(processed_buckets[event_type]):
            bucket = processed_buckets[event_type][bwr]
            print(f'{bucket["win_rate"]},{bucket["average_gems"]},{bucket["average_packs"]},{bucket["draft_count"]}')


def get_matches(file_name):
    # strong assumptions about the order of data
    #  each draft must be contiguous lines in the input
    #   (this could be worked around if needed, using the draft identifier)
    #  each match within each draft must be contiguous, and in game order
    #   (no way to fix this -- there aren't any match identifiers)
    with open(file_name) as f:
        i = 0
        headers = f.readline().split(",")
        user_win_rate_bucket_index = headers.index("user_win_rate_bucket")
        user_n_games_bucket_index = headers.index("user_n_games_bucket")
        draft_id_index = headers.index("draft_id")
        expansion_index = headers.index("expansion")
        event_type_index = headers.index("event_type")
        game_number_index = headers.index("game_number")
        rank_index = headers.index("rank")
        opp_rank_index = headers.index("opp_rank") if "opp_rank" in headers else -1
        won_index = headers.index("won")
        prev_game_number = 99
        wins_this_match = 0
        losses_this_match = 0
        matches = []
        for line in f:
            split = line.split(",")
            user_n_games_bucket = split[user_n_games_bucket_index]
            event_type = split[event_type_index]
            if int(user_n_games_bucket) < min_matches[event_type]:
                continue
            user_win_rate_bucket = split[user_win_rate_bucket_index]
            draft_id = split[draft_id_index]
            expansion = split[expansion_index]
            game_number = split[game_number_index]
            rank = split[rank_index]
            opp_rank = split[opp_rank_index] if opp_rank_index >= 0 else ""
            won = split[won_index]

            if event_type == "TradDraft":
                if int(game_number) > 3:
                    print(f'WARN: more than 3 games in a match for {expansion} TradDraft {draft_id}')
                    # this is a probably from a draw? but the data doesn't differentiate so not sure how to handle it
                    continue
                if int(game_number) <= prev_game_number:
                    # Either we recorded the match already last time through the loop,
                    #  or the match wasn't fully recorded by the 17lands client and we are okay discarding it.
                    # Either way, start the next match fresh:
                    wins_this_match = 0
                    losses_this_match = 0

                if won == "True":
                    wins_this_match += 1
                elif won == "False":
                    losses_this_match += 1
                else:
                    raise Exception("Unexpected: " + won + " in " + draft_id)

                if wins_this_match == 2:
                    outcome = "win"
                elif losses_this_match == 2:
                    outcome = "loss"
                else:
                    outcome = "not done yet"
            elif event_type == "PremierDraft":
                outcome = "win" if won == "True" else ("loss" if won == "False" else "bad data")
            else:
                raise Exception("Unknown event type " + event_type + " in " + draft_id)

            if outcome != "not done yet":
                match = {
                    "draft_id": draft_id,
                    "user_win_rate_bucket": round(float(user_win_rate_bucket), 2),
                    "expansion": expansion,
                    "event_type": event_type,
                    "rank": rank.split("-")[0],
                    "subrank": rank.split("-")[1] if "-" in rank else "",
                    "opp_rank": opp_rank.split("-")[0],
                    "won": outcome == "win",
                }
                matches.append(match)

            prev_game_number = int(game_number)

            i += 1
            if i % 50000 == 0:
                print(".", end="")
            if i > 20000000:
                print("hit limit")
                return matches
    print("")
    return matches


def get_drafts(matches):
    drafts = []
    prev_draft = {
        "draft_id": "abcd",
        "event_type": "abcd",
    }
    wins_this_draft = 0
    losses_this_draft = 0
    for match in matches:
        if prev_draft["draft_id"] != match["draft_id"]:
            draft_complete = False
            if prev_draft["event_type"] == "PremierDraft":
                # todo: handle draws. are they reported as won==False? need to subtract losses for events with too many?
                if wins_this_draft == 7 or losses_this_draft == 3:
                    draft_complete = True
                if wins_this_draft > 7:
                    print(f'WARN: {wins_this_draft} wins in {prev_draft["expansion"]} PremierDraft {prev_draft["draft_id"]}')
                    draft_complete = False
                if losses_this_draft > 3:
                    print(f'WARN: {losses_this_draft} losses in {prev_draft["expansion"]} PremierDraft {prev_draft["draft_id"]}')
                    draft_complete = False
                if wins_this_draft == 7 and losses_this_draft == 3:
                    print(f'WARN: 7 wins *and* 3 losses in {prev_draft["expansion"]} PremierDraft {prev_draft["draft_id"]}')
                    draft_complete = False
            if prev_draft["event_type"] == "TradDraft":
                if wins_this_draft + losses_this_draft == 3 or (wins_this_draft == 0 and losses_this_draft == 2):
                    draft_complete = True
                if wins_this_draft + losses_this_draft > 3:
                    print(f'WARN: more than 3 matches played in {prev_draft["expansion"]} TradDraft {prev_draft["draft_id"]}')
                    draft_complete = False
            if draft_complete:
                drafts.append(prev_draft)
            wins_this_draft = 0
            losses_this_draft = 0

        if match["won"]:
            wins_this_draft += 1
        else:
            losses_this_draft += 1

        prev_draft = {
            "draft_id": match["draft_id"],
            "user_win_rate_bucket": match["user_win_rate_bucket"],
            "expansion": match["expansion"],
            "event_type": match["event_type"],
            "rank": match["rank"],
            "subrank": match["subrank"],
            "opp_rank": match["opp_rank"],
            "wins": wins_this_draft,
            "losses": losses_this_draft,
        }

    return drafts


def get_buckets(drafts):
    # no assumptions about the order of data within each bucket
    buckets = defaultdict(lambda: defaultdict(lambda: {
        "draft_count": 0,
        "total_gems": 0,
        "total_packs": 0,
        "total_match_wins": 0,
        "total_match_losses": 0,
    }))
    for draft in drafts:
        bucket = buckets[draft["event_type"]][draft["user_win_rate_bucket"]]
        bucket["draft_count"] += 1
        bucket["total_gems"] += gems[draft["event_type"]][draft["wins"]]
        bucket["total_packs"] += packs[draft["event_type"]][draft["wins"]]
        bucket["total_match_wins"] += draft["wins"]
        bucket["total_match_losses"] += draft["losses"]
    return buckets


def process_buckets(buckets):
    processed_buckets = defaultdict(lambda: defaultdict(lambda: {}))
    for event_type in buckets:
        for bwr in buckets[event_type]:
            bucket = buckets[event_type][bwr]
            draft_count = bucket["draft_count"]
            wins = bucket["total_match_wins"]
            losses = bucket["total_match_losses"]
            avg_gems = bucket["total_gems"] / draft_count if draft_count > 0 else "TBD"
            avg_packs = bucket["total_packs"] / draft_count if draft_count > 0 else "TBD"
            processed_buckets[event_type][bwr] = {
                "draft_count": draft_count,
                "average_gems": avg_gems,
                "average_packs": avg_packs,
                "win_rate": wins / (wins + losses) if wins + losses > 0 else "TBD",
            }

        processed_buckets[event_type][0.0] = {
            "draft_count": 0,
            "average_gems": 0,
            "average_packs": 0,
            "win_rate": 0.0,
        }
        processed_buckets[event_type][1.0] = {
            "draft_count": 0,
            "average_gems": max(gems[event_type].values()),
            "average_packs": max(packs[event_type].values()),
            "win_rate": 1.0,
        }

    return processed_buckets


if __name__ == "__main__":
    main()
