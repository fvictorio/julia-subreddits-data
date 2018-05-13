using CSV
using DataFrames

filenames = [
  "reddit_traffic/art_traffic.csv"
  "reddit_traffic/askreddit_traffic.csv"
  "reddit_traffic/askscience_traffic.csv"
  "reddit_traffic/aww_traffic.csv"
  "reddit_traffic/blog_traffic.csv"
  "reddit_traffic/books_traffic.csv"
  "reddit_traffic/comics_traffic.csv"
  "reddit_traffic/creepy_traffic.csv"
  "reddit_traffic/dataisbeautiful_traffic.csv"
  "reddit_traffic/datasets_traffic.csv"
  "reddit_traffic/diy_traffic.csv"
  "reddit_traffic/documentaries_traffic.csv"
  "reddit_traffic/earthporn_traffic.csv"
  "reddit_traffic/explainlikeimfive_traffic.csv"
  "reddit_traffic/food_traffic.csv"
  "reddit_traffic/funny_traffic.csv"
  "reddit_traffic/futurology_traffic.csv"
  "reddit_traffic/gadgets_traffic.csv"
  "reddit_traffic/gaming_traffic.csv"
  "reddit_traffic/getmotivated_traffic.csv"
  "reddit_traffic/gifs_traffic.csv"
  "reddit_traffic/gis_traffic.csv"
  "reddit_traffic/history_traffic.csv"
  "reddit_traffic/iama_traffic.csv"
  "reddit_traffic/internetisbeautiful_traffic.csv"
  "reddit_traffic/jokes_traffic.csv"
  "reddit_traffic/lifeprotips_traffic.csv"
  "reddit_traffic/listentothis_traffic.csv"
  "reddit_traffic/mapporn_traffic.csv"
  "reddit_traffic/mildlyinteresting_traffic.csv"
  "reddit_traffic/movies_traffic.csv"
  "reddit_traffic/music_traffic.csv"
  "reddit_traffic/news_traffic.csv"
  "reddit_traffic/nosleep_traffic.csv"
  "reddit_traffic/nottheonion_traffic.csv"
  "reddit_traffic/oldschoolcool_traffic.csv"
  "reddit_traffic/personalfinance_traffic.csv"
  "reddit_traffic/philosophy_traffic.csv"
  "reddit_traffic/photoshopbattles_traffic.csv"
  "reddit_traffic/pics_traffic.csv"
  "reddit_traffic/science_traffic.csv"
  "reddit_traffic/showerthoughts_traffic.csv"
  "reddit_traffic/space_traffic.csv"
  "reddit_traffic/sports_traffic.csv"
  "reddit_traffic/statistics_traffic.csv"
  "reddit_traffic/television_traffic.csv"
  "reddit_traffic/tifu_traffic.csv"
  "reddit_traffic/todayilearned_traffic.csv"
  "reddit_traffic/upliftingnews_traffic.csv"
  "reddit_traffic/videos_traffic.csv"
  "reddit_traffic/worldnews_traffic.csv"
]

transforms = Dict(
  "pull_timestamp" => x -> DateTime(x, "yyyy-mm-dd HH:SS:MM")
)

types = Dict(
  "users_here" => String
)

loadCSV = filename -> CSV.read(filename, transforms = transforms, types = types)

csvs = map(filenames[:]) do filename
  df = loadCSV(filename)

  usersHere = map(df[:users_here]) do x
    if (x == "{}" || x == "")
      missing
    else
      parse(x)
    end
  end

  df[:users_here] = usersHere

  df
end

# concatenate all csvs
csv = reduce((df1, df2) -> [df1; df2], csvs)
csv[:users_here_perc] = csv[:users_here] ./ csv[:subscribers]

data = by(csv, :subreddit, df -> DataFrame(
  subscribers = maximum(skipmissing(df[:subscribers])),
  users_here = mean(skipmissing(df[:users_here_perc]))
))

topSubscribers = sort(data, :subscribers, rev = true)
topSubscribers[:position] = 1:size(topSubscribers, 1)

topUsersHere = sort(data, :users_here, rev = true)
topUsersHere[:position] = 1:size(topUsersHere, 1)

positions = join(topSubscribers, topUsersHere, on = :subreddit, makeunique=true)
delete!(positions, :subscribers_1)
delete!(positions, :users_here_1)
rename!(positions, :position => :topSubscribers)
rename!(positions, :position_1 => :topUsersHere)

positions[:topDifference] = positions[:topSubscribers] - positions[:topUsersHere]
positions[:absTopDifference] = abs.(positions[:topDifference])
sort!(positions, :absTopDifference, rev=true)
delete!(positions, :absTopDifference)

CSV.write("positions.csv", positions)

nothing
