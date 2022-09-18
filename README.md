# Playlist Manager
Script for managing youtube playlists.

Behavior:
* Enforces a maximum number of items in playlists (YouTube playlists over 100 elements are very painful to scroll through).
* Synchronizes playlists order with ground truth in google sheet.
* Removes duplicate videos.
* Adds missing videos.
* Builds archive playlists for overflow above 100.
* Records playlists in sheet.

## Sheet Examples
TODO: Add sample sheets that include both the video metadata as well as the playlist data.

## Issues
Youtube has a max playlists per day "hidden" quota... ideally running this periodically will prevent the need for creating too many at once.

## Installation

## Authentication
See [Auth Manager](https://github.com/aldenpotamus/auth_manager)

## Usage
