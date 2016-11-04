package testing

import (
	"net/http"
	"strings"
)

var Recent = `
<feed
    xmlns="http://www.w3.org/2005/Atom">
    <title>Event store feed</title>
    <id>recent</id>
    <link rel="self" href="http://localhost:5000/notifications/recent"></link>
    <link rel="related" href="http://localhost:5000/notifications/recent"></link>
    <link rel="prev-archive" href="http://localhost:5000/notifications/9BC3EA7D-51E2-8C61-0E08-02368CD22054"></link>
    <updated>2016-10-31T11:05:28-07:00</updated>
    <entry>
        <title>event</title>
        <id>urn:esid:9c5f255c-c5f2-42cb-7f06-5be564e91fd9:1</id>
        <link rel="self" href="http://localhost:5000/notifications/9c5f255c-c5f2-42cb-7f06-5be564e91fd9/1"></link>
        <published>2016-10-31T11:03:05.232441-07:00</published>
        <updated></updated>
        <content type="TACRE">CiQ5YzVmMjU1Yy1jNWYyLTQyY2ItN2YwNi01YmU1NjRlOTFmZDkSBWZvbyA3GgViYXIgNyITYmF6ICUhZChNSVNTSU5HKSwgaQ==</content>
    </entry>
    <entry>
        <title>event</title>
        <id>urn:esid:1f454e71-42f9-4d88-6979-ae643aa88cdd:1</id>
        <link rel="self" href="http://localhost:5000/notifications/1f454e71-42f9-4d88-6979-ae643aa88cdd/1"></link>
        <published>2016-10-31T11:03:05.224863-07:00</published>
        <updated></updated>
        <content type="TACRE">CiQxZjQ1NGU3MS00MmY5LTRkODgtNjk3OS1hZTY0M2FhODhjZGQSBWZvbyA1GgViYXIgNSITYmF6ICUhZChNSVNTSU5HKSwgaQ==</content>
    </entry>
</feed>`

var SecondArchive = `
<feed
    xmlns="http://www.w3.org/2005/Atom">
    <title>Event store feed</title>
    <id>9BC3EA7D-51E2-8C61-0E08-02368CD22054</id>
    <link rel="self" href="http://localhost:5000/notifications/9BC3EA7D-51E2-8C61-0E08-02368CD22054"></link>
    <link rel="prev-archive" href="http://localhost:5000/notifications/9AF82230-6137-4DA3-3580-80EDA74B0DE2"></link>
    <link rel="next-archive" href="http://localhost:5000/notifications/recent"></link>
    <updated></updated>
    <entry>
        <title>event</title>
        <id>urn:esid:9f02eae0-bf8c-46c1-7afb-9af83616b0ae:1</id>
        <link rel="self" href="http://localhost:5000/notifications/9f02eae0-bf8c-46c1-7afb-9af83616b0ae/1"></link>
        <published>2016-10-31T11:03:05.215076-07:00</published>
        <updated></updated>
        <content type="TACRE">CiQ5ZjAyZWFlMC1iZjhjLTQ2YzEtN2FmYi05YWY4MzYxNmIwYWUSBWZvbyAzGgViYXIgMyITYmF6ICUhZChNSVNTSU5HKSwgaQ==</content>
    </entry>
    <entry>
        <title>event</title>
        <id>urn:esid:f3234d82-0cff-4221-64de-315c8ab6dbd6:1</id>
        <link rel="self" href="http://localhost:5000/notifications/f3234d82-0cff-4221-64de-315c8ab6dbd6/1"></link>
        <published>2016-10-31T11:03:05.206026-07:00</published>
        <updated></updated>
        <content type="TACRE">CiRmMzIzNGQ4Mi0wY2ZmLTQyMjEtNjRkZS0zMTVjOGFiNmRiZDYSBWZvbyA0GgViYXIgNCITYmF6ICUhZChNSVNTSU5HKSwgaQ==</content>
    </entry>
    <entry>
        <title>event</title>
        <id>urn:esid:3418b971-0ea8-483d-4520-9bfbc6a1d356:1</id>
        <link rel="self" href="http://localhost:5000/notifications/3418b971-0ea8-483d-4520-9bfbc6a1d356/1"></link>
        <published>2016-10-31T11:03:05.19834-07:00</published>
        <updated></updated>
        <content type="TACRE">CiQzNDE4Yjk3MS0wZWE4LTQ4M2QtNDUyMC05YmZiYzZhMWQzNTYSBWZvbyA2GgViYXIgNiITYmF6ICUhZChNSVNTSU5HKSwgaQ==</content>
    </entry>
</feed>
`

var FirstArchive = `
<feed
    xmlns="http://www.w3.org/2005/Atom">
    <title>Event store feed</title>
    <id>9AF82230-6137-4DA3-3580-80EDA74B0DE2</id>
    <link rel="self" href="http://localhost:5000/notifications/9AF82230-6137-4DA3-3580-80EDA74B0DE2"></link>
    <link rel="next-archive" href="http://localhost:5000/notifications/9BC3EA7D-51E2-8C61-0E08-02368CD22054"></link>
    <updated></updated>
    <entry>
        <title>event</title>
        <id>urn:esid:e44afbe7-e24f-4bdf-4fa8-9cfc46e4c496:1</id>
        <link rel="self" href="http://localhost:5000/notifications/e44afbe7-e24f-4bdf-4fa8-9cfc46e4c496/1"></link>
        <published>2016-10-31T11:03:05.182819-07:00</published>
        <updated></updated>
        <content type="TACRE">CiRlNDRhZmJlNy1lMjRmLTRiZGYtNGZhOC05Y2ZjNDZlNGM0OTYSBWZvbyAwGgViYXIgMCITYmF6ICUhZChNSVNTSU5HKSwgaQ==</content>
    </entry>
    <entry>
        <title>event</title>
        <id>urn:esid:3a56b98b-0a03-4822-44c7-93216255d857:1</id>
        <link rel="self" href="http://localhost:5000/notifications/3a56b98b-0a03-4822-44c7-93216255d857/1"></link>
        <published>2016-10-31T11:03:05.169555-07:00</published>
        <updated></updated>
        <content type="TACRE">CiQzYTU2Yjk4Yi0wYTAzLTQ4MjItNDRjNy05MzIxNjI1NWQ4NTcSBWZvbyAyGgViYXIgMiITYmF6ICUhZChNSVNTSU5HKSwgaQ==</content>
    </entry>
    <entry>
        <title>event</title>
        <id>urn:esid:cee18efc-0568-48f9-764c-149085ea0324:1</id>
        <link rel="self" href="http://localhost:5000/notifications/cee18efc-0568-48f9-764c-149085ea0324/1"></link>
        <published>2016-10-31T11:03:05.161535-07:00</published>
        <updated></updated>
        <content type="TACRE">CiRjZWUxOGVmYy0wNTY4LTQ4ZjktNzY0Yy0xNDkwODVlYTAzMjQSBWZvbyAxGgViYXIgMSITYmF6ICUhZChNSVNTSU5HKSwgaQ==</content>
    </entry>
</feed>
`

var EmptyFeed = `
<feed
    xmlns="http://www.w3.org/2005/Atom">
    <title>Event store feed</title>
    <id>recent</id>
    <link rel="self" href="http://localhost:5000/notifications/recent"></link>
    <link rel="related" href="http://localhost:5000/notifications/recent"></link>
    <updated>2016-11-04T07:35:49-07:00</updated>
</feed>
`

var EmptryFeedHandler = func(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte(EmptyFeed))
}

var RecentHandler = func(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte(Recent))
}

var GetFeedHandler = func(w http.ResponseWriter, r *http.Request) {

	parts := strings.Split(r.URL.Path, "/")
	feedid := parts[len(parts)-1]

	var feedData string
	switch feedid {
	case "recent":
		feedData = Recent
	case "9BC3EA7D-51E2-8C61-0E08-02368CD22054":
		feedData = SecondArchive
	case "9AF82230-6137-4DA3-3580-80EDA74B0DE2":
		feedData = FirstArchive
	}

	if feedData == "" {
		http.Error(w, "", http.StatusNotFound)
		return
	}

	w.Write([]byte(feedData))
}
