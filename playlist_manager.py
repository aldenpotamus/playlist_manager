import configparser
import pygsheets
import sys
import copy
import collections
import math

sys.path.append("..")
from auth_manager.auth_manager import AuthManager

simulatedQuota = 0

def main():
    # Pull Playlist Data and Run Information from Sheet
    playlistData = getPlaylistDataFromSheet()

    # Pull Current Playlist Infomation
    getPlaylistData(playlistData)

    # Pull Video Data from Sheet
    videoData = getDataFromSheet(playlistData.keys())

    # Build Migration Change List
    playlistsToCreate = buildMigrationPlan(videoData, playlistData)

    # Create New Playlists
    createArchivalPlaylists(playlistData, playlistsToCreate)

    for game in playlistData.keys():
        print(f'Processing Playlists for [{game}]')
        # Check for Duplicate Videos in Playlist
        videosToRemove = scanForNonMemberVideos(videoData[game], playlistData[game])

        # Check for Missing Videos in Playlist
        videosToAdd = scanForMissingVideos(videoData[game], playlistData[game])

        # Check for Ordering Issues in Playlist
        videosToMove = scanForOrderingIssues(videoData[game], playlistData[game])

        for playlistId in videosToRemove:
            removeDuplicateVideos(playlistId, videosToRemove[playlistId])
        for playlistId in videosToAdd:
            addMissingVideos(playlistId, videosToAdd[playlistId])
        for playlistId in videosToMove:
            adjustVideoOrder(playlistId, videosToMove[playlistId])

    if CONFIG.getboolean('GENERAL', 'testMode'):
        global simulatedQuota
        print(f'Quota Estimate for this operation is {simulatedQuota}.')

    return

def getPlaylistDataFromSheet():
    print('Get Playlist and Run Data from Sheet...')
    playlistWorksheet = sheet.worksheet_by_title(CONFIG['SHEET']['playlistSheetName'])

    headers = playlistWorksheet.get_values(start='A2', end='Z2', include_tailing_empty=False, returnas='matrix')[0]
    playlists = playlistWorksheet.get_values(start='A4', end='Z100', include_tailing_empty=False, returnas='matrix')

    playlistData = [{key: value for (key,value) in zip(headers, p)} for p in playlists]
    
    gameToCheck = {p['playlist_game']: True for p in playlistData if p['to_process'] == 'TRUE'}

    playlistData = [p for p in playlistData if p['playlist_game'] in gameToCheck]
    playlistDataDict = {ps['playlist_game']: [p for p in playlistData if p['playlist_game'] == ps['playlist_game']] for ps in playlistData}

    print(f'\tSheet has {len(gameToCheck.keys())} game totalling {len(playlistData)} playlists flagged for processing.')

    return playlistDataDict

def getPlaylistData(playlistData):
    print('Get Playlist Data from YouTube...')

    for playlists in playlistData.values():
        for playlist in playlists:
            playlist['videoList'] = []

            playlistItemsRequest = youtube.playlistItems().list(
                part="snippet,contentDetails",
                maxResults=50,
                playlistId=playlist['playlist_id']
            )
            while playlistItemsRequest:
                playlistItemsResponse = playlistItemsRequest.execute()
                playlist['videoList'].extend([ trimPlaylistItem(pli) for pli in playlistItemsResponse['items'] ])
                playlistItemsRequest = youtube.playlistItems().list_next(playlistItemsRequest, playlistItemsResponse)

    print(f'\tRetreived playlist items from YouTube.')
    return

def trimPlaylistItem(playlistItem):
    return {
        'playlistItemId': playlistItem['id'],
        'videoId': playlistItem['snippet']['resourceId']['videoId'],
        'position': playlistItem['snippet']['position']
    }

def getDataFromSheet(gamesToProcess):
    print('Get Episode Data from Sheet...')

    videosWorksheet = sheet.worksheet_by_title(CONFIG['SHEET']['videoSheetName'])
    headers = videosWorksheet.get_values(start='A2', end='Z2', include_tailing_empty=False, returnas='matrix')[0]
    videos = videosWorksheet.get_values(start='A5', end='Z10000', include_tailing_empty=False, returnas='matrix')

    fields = ['special', 'episode_number', 'videoid', 'game']
    videoData = [{key: value for (key,value) in zip(headers, v) if key in fields} for v in videos]
    videoData = [v for v in videoData if v['game'] in gamesToProcess and v['special'] != 'TRUE']

    videoDataGrouped = {}
    for video in videoData:
        if video['game'] not in videoDataGrouped.keys():
            videoDataGrouped[video['game']] = []
        videoDataGrouped[video['game']].append(video)

    for game in videoDataGrouped.keys():
        for pos, vid in enumerate(videoDataGrouped[game]):
            vid['position'] = pos

    print(f'\tRetreived {len(videoData)} video data entries.')

    return videoDataGrouped

def differentiatePlaylists(playlists):
    mainPlaylist = [p for p in playlists if p['playlist_type'] == 'MAIN']
    if len(mainPlaylist) != 1:
        print('\t\tToo many main playlists for game, exiting...')
        sys.exit()
    else:
        mainPlaylist = mainPlaylist[0]

    archives = [p for p in playlists if p['playlist_type'] == 'ARCHIVAL']

    return mainPlaylist, archives

def scanForNonMemberVideos(videoData, playlists):
    print('\tChecking for Videos that don\'t Belong in Playlists...')
    mainPlaylistStart = 0

    playlistItemsToRemove = {}
    mainPlaylist, archives = differentiatePlaylists(playlists)

    for archivePlaylist in archives:
        allVideoData = {v['videoid']: True for v in videoData[int(archivePlaylist['vid_index_start']):int(archivePlaylist['vid_index_end'])]}
        mainPlaylistStart = max(mainPlaylistStart, int(archivePlaylist['vid_index_end']))
        
        detectNonMembers(allVideoData, archivePlaylist, playlistItemsToRemove)

    allVideoIds = {v['videoid']: True for v in videoData[mainPlaylistStart:]}
    detectNonMembers(allVideoIds, mainPlaylist, playlistItemsToRemove)

    print(f'\t\tFound {sum([len(playlistItemsToRemove[k]) for k in playlistItemsToRemove.keys()])} duplicate videos across playlists.')
    return playlistItemsToRemove

def detectNonMembers(allVideoIds, playlist, playlistItemsToRemove):
    print(f'\t\tProcessing Playlist: {playlist["playlist_title"]}')
    remPos = [] # Position of duplicate elements
    
    seenVideoIds = {}
    for pos, playlistItem in enumerate(playlist['videoList']):
        if playlistItem['videoId'] in seenVideoIds.keys():
            print(f'\t\t\tDuplicate Video Found [{playlistItem["videoId"]}]: Playlistitem [{playlistItem["playlistItemId"]}] Queued for Removal')
            if playlist['playlist_id'] not in playlistItemsToRemove:
                playlistItemsToRemove[playlist['playlist_id']] = []
            playlistItemsToRemove[playlist['playlist_id']].append(playlistItem['playlistItemId'])
            remPos.insert(0, pos)
        if playlistItem['videoId'] not in allVideoIds.keys():
            print(f'\t\t\tNon-member Video Found [{playlistItem["videoId"]}]: Playlistitem [{playlistItem["playlistItemId"]}] Queued for Removal')
            if playlist['playlist_id'] not in playlistItemsToRemove:
                playlistItemsToRemove[playlist['playlist_id']] = []
            playlistItemsToRemove[playlist['playlist_id']].append(playlistItem['playlistItemId'])
            remPos.insert(0, pos)

        seenVideoIds[playlistItem['videoId']] = True

    # Update downstream positions for order check
    for p in remPos:
        del playlist['videoList'][p]

def scanForMissingVideos(videoData, playlists):
    print('\tChecking for Missing Videos in Playlists...')
    missingVidsCount = 0
    mainPlaylistStart = 0

    videosToAdd = {}
    mainPlaylist, archives = differentiatePlaylists(playlists)

    for archivePlaylist in archives:
        print(f'\t\tScannign playlist [{archivePlaylist["playlist_title"]}] for missing videos...')
        mainPlaylistStart = max(mainPlaylistStart, int(archivePlaylist['vid_index_end']))
        
        for pos, video in enumerate(videoData[int(archivePlaylist['vid_index_start']):int(archivePlaylist['vid_index_end'])]):
            if video['videoid'] not in [v['videoId'] for v in archivePlaylist['videoList']]:
                print(f'\t\t\tVideo [{video["videoid"]}] is missing from Playlist {archivePlaylist["playlist_id"]}.')
                if archivePlaylist['playlist_id'] not in videosToAdd:
                    videosToAdd[archivePlaylist['playlist_id']] = []
                videosToAdd[archivePlaylist['playlist_id']].append((pos, video['videoid']))

                # Update downstream positions for order check
                for playlistItem in archivePlaylist['videoList']:
                    if int(playlistItem['position']) >= pos:
                        playlistItem['position'] += 1

                archivePlaylist['videoList'].insert(pos, {
                    'playlistItemId': 'TO ADD',
                    'videoId': video['videoid'],
                    'position': pos
                })
                missingVidsCount += 1

    # Process main playlist
    print(f'\t\tScannign playlist [{mainPlaylist["playlist_title"]}] for missing videos...')

    for pos, video in enumerate(videoData[mainPlaylistStart:]):
        if video['videoid'] not in [v['videoId'] for v in mainPlaylist['videoList']]:
            print(f'\t\t\tVideo [{video["videoid"]}] is missing from playlist {mainPlaylist["playlist_id"]}.')
            if mainPlaylist['playlist_id'] not in videosToAdd:
                videosToAdd[mainPlaylist['playlist_id']] = []
            videosToAdd[mainPlaylist['playlist_id']].append((pos, video['videoid']))

            # Update downstream positions for order check
            for playlistItem in mainPlaylist['videoList']:
                if int(playlistItem['position']) >= pos:
                    playlistItem['position'] += 1

            mainPlaylist['videoList'].insert(video['position'], {
                'playlistItemId': 'TO ADD',
                'videoId': video['videoid'],
                'position': pos
            })
            missingVidsCount += 1    

    print(f'\t\tFound {missingVidsCount} missing videos from playlist.')

    return videosToAdd

def scanForOrderingIssues(videoData, playlists):
    print('\tChecking for Ordering Issues in Playlists...')

    mainPlaylist, archives = differentiatePlaylists(playlists)

    mainPlaylistStart = 0

    moves = {}
    for archivePlaylist in archives:
        print(f'\t\tChecking playlists {archivePlaylist["playlist_title"]}...')
        mainPlaylistStart = max(mainPlaylistStart, int(archivePlaylist['vid_index_end']))
        plVideoData = videoData[int(archivePlaylist['vid_index_start']):int(archivePlaylist['vid_index_end'])]

        solvePlaylistOrder(plVideoData, archivePlaylist, moves)

    print(f'\t\tChecking playlists {mainPlaylist["playlist_title"]}...')
    plVideoData = videoData[mainPlaylistStart:]
    
    solvePlaylistOrder(plVideoData, mainPlaylist, moves)

    return moves

def solvePlaylistOrder(videoData, playlist, moves):
    global bestSolutionSize

    if len(videoData) != len(playlist['videoList']):
        print(f'\t\t\tOrder check failed due to length mismatch [PLAYLIST: {len(playlist)} vs SHEET: {len(videoData)}]... exiting.')
        sheetVideoIds = [v['videoid'] for v in videoData]
        plVideoIds = [p['videoId'] for p in playlist['videoList']]
        print(f'\t\t\t\tExtra in Sheets {set(sheetVideoIds)-set(plVideoIds)}')
        print(f'\t\t\t\tExtra in Playlist {set(plVideoIds)-set(sheetVideoIds)}')
        print(f'\t\t\t\tDuplicates in VideoSheet {[item for item, count in collections.Counter(sheetVideoIds).items() if count > 1]}')

        sys.exit()

    idToPos = { v['videoid']: pos for pos, v in enumerate(videoData) }

    bestSolutionSize = len(playlist['videoList'])
    moves[playlist['playlist_id']] = scanForOrderingIssuesHelper([{'currentPosition': pos,
                                                                   'destPosition': idToPos[pli['videoId']],
                                                                   'playlistItem': pli,
                                                                  } for (pos, pli) in enumerate(playlist['videoList'])],
                                                                 [])

def scanForOrderingIssuesHelper(playlist, moves):
    global bestSolutionSize
    if len(moves) >= bestSolutionSize:
        # print('\t\t\tAbandoning branch due to length...')
        return None

    for pos, pli in enumerate(playlist):
        pli['currentPosition'] = pos

    outOfPosition = [p for p in playlist if p['currentPosition'] != p['destPosition']]
    outOfPosition.sort(reverse=True, key=distHeuristic)

    if len(outOfPosition) == 0:
        print(f'\t\t\tSolution Found [MOVES: {len(moves)}]')
        bestSolutionSize = min(len(moves), bestSolutionSize)
        return moves

    bestMoves = None
    for pliToMove in outOfPosition[:CONFIG.getint('GENERAL', 'maxSearchBranchFactor')]:
        if pliToMove['playlistItem']['videoId'] in [m[2] for m in moves]:
            # print('\t\t\tMoving a video for the second time... end exploration.')
            continue

        newMoves = copy.copy(moves)
        newMoves.append((pliToMove['destPosition'], pliToMove['playlistItem']['playlistItemId'], pliToMove['playlistItem']['videoId']))
        newPlaylist = copy.copy(playlist)
        newPlaylist.insert(pliToMove['destPosition'], newPlaylist.pop(pliToMove['currentPosition']))
        potentailMoves = scanForOrderingIssuesHelper(newPlaylist, newMoves)

        if bestMoves is not None and potentailMoves is not None:
            # print('\t\t\tComparing Two Working Solutions...')
            bestMoves = bestMoves if len(bestMoves) < len(potentailMoves) else potentailMoves
        elif bestMoves is None and potentailMoves is not None:
            # print(f'\t\t\tNew Solution Set As Best... [{potentailMoves}]')
            bestMoves = potentailMoves

    return bestMoves

def distHeuristic(pli):
    return abs(pli['currentPosition'] - pli['destPosition'])

def removeDuplicateVideos(playlistId, videosToRemove):
    print('\tRemoving Duplicate Videos in Playlists...')

    for deletionPLI in videosToRemove:
        print(f'\t\tDeleting playlist item [{deletionPLI}] to playlist [{playlistId}]...')
        ytRemPlaylistItemFromplaylist(deletionPLI)

    return

def ytRemPlaylistItemFromplaylist(playlistItemId):
    if not CONFIG.getboolean('GENERAL', 'testMode'):
        request = youtube.playlistItems().delete(
            id=playlistItemId
        )
        request.execute()
    else:
        print(f'\t\t\tSkipping API call youtube.playlistItems().delete(id={playlistItemId})')
        
        global simulatedQuota
        simulatedQuota += 50  

def addMissingVideos(playlistId, videosToAdd):
    print('\tAdding Missing Videos to Playlists...')

    for pos, additionPLI in videosToAdd:
        print(f'\t\tCreating playlistItem for video [{additionPLI}] to playlist [{playlistId}] at position {pos}...')
        ytAddPlaylistItemToPlaylist(playlistId, additionPLI, pos)

    return

def ytAddPlaylistItemToPlaylist(playlistId, videoId, position):
    if not CONFIG.getboolean('GENERAL', 'testMode'):
        request = youtube.playlistItems().insert(
            part="snippet",
            body={
                "snippet": {
                    "playlistId": playlistId,
                    "position": position,
                    "resourceId": {
                        "kind": "youtube#video",
                        "videoId": videoId
                    }
                }
            }
        )
        response = request.execute()
    else:
        print(f'\t\t\tSkipping API call youtube.playlistItems().insert(playlistId={playlistId}, '\
                                                                   f'position={position}, '\
                                                                   f'videoId={videoId})')
        
        global simulatedQuota
        simulatedQuota += 50

def adjustVideoOrder(playlistId, movesToMake):
    print('\tMoving Improperly Positioned Videos in Playlists...')

    if movesToMake is None or not len(movesToMake):
        return

    for pos, movePLI, videoIdPLI in movesToMake:
        print(f'\t\tMoving playlistItem for video [{videoIdPLI}] to position [{pos}]...')
        ytUpdatePlaylistItemPosition(playlistId, movePLI, pos, videoIdPLI)

    return

def ytUpdatePlaylistItemPosition(playlistId, playlistItemId, position, videoId):
    if not CONFIG.getboolean('GENERAL', 'testMode'):
        request = youtube.playlistItems().update(
            part="snippet",
            body={
                "id": playlistItemId,
                "snippet": {
                    "playlistId": playlistId,
                    "position": position,
                    "resourceId": {
                        "kind": "youtube#video",
                        "videoId": videoId
                    }
                }
            }
        )
        response = request.execute()
    else:
        print(f'\t\t\tSkipping API call youtube.playlistItems().update(playlistId={playlistId}, '\
                                                                   f'playlistItemId={playlistItemId}, '\
                                                                   f'position={position}, '\
                                                                   f'videoId={videoId})')
        
        global simulatedQuota
        simulatedQuota += 50

def buildMigrationPlan(videoData, playlistData):
    print('Building Migration Plan for Playlists...')

    playlistsToCreate = []

    maxPlaylistSize = CONFIG.getint('GENERAL', 'maxPlaylistSize')

    for game in playlistData.keys():
        mainPlaylist, archives = differentiatePlaylists(playlistData[game])

        playListItems = mainPlaylist['videoList']
        if len(playListItems) > maxPlaylistSize:
            print(f'\tPlaylist [{game}] [{len(playListItems)}], migration required...')

            endSplit = (len(videoData[game])+1) - maxPlaylistSize
            while int(videoData[game][endSplit]['episode_number']) == int(videoData[game][endSplit-1]['episode_number']):
                endSplit += 1

            videoDataForArchive = videoData[game][:endSplit]

            previousSplitPoint = 0
            neededPlaylists = math.floor(len(videoDataForArchive)/maxPlaylistSize) + 1

            for i in range(neededPlaylists):
                splitPoint = (i + 1) * maxPlaylistSize
                
                if previousSplitPoint == len(videoDataForArchive):
                    print('\t\tDone... not more playlists needs...')
                    break
                
                if splitPoint < (len(videoDataForArchive)-1):
                    while int(videoDataForArchive[splitPoint]['episode_number']) == int(videoDataForArchive[splitPoint+1]['episode_number']):
                        splitPoint -= 1
                else:
                    splitPoint = len(videoDataForArchive) - 1

                playlistTitle = game + f' Episodes {videoDataForArchive[previousSplitPoint]["episode_number"]}-' \
                                                 f'{videoDataForArchive[splitPoint]["episode_number"]} [ARCHIVE]'
                playlistsToCreate.append({
                    'to_process': 'FALSE',
                    'playlist_title': playlistTitle,
                    'playlist_type': 'ARCHIVAL',
                    'playlist_game': game,
                    'archive_num': (i + 1),
                    'vid_index_start': max(0, previousSplitPoint-1),
                    'vid_index_end': splitPoint,
                    'ep_start': videoDataForArchive[previousSplitPoint]["episode_number"],
                    'ep_end': videoDataForArchive[splitPoint]["episode_number"],
                    'videos': mainPlaylist['videoList'][previousSplitPoint:splitPoint]
                })
                
                print(f'\t\tCreating placeholder playlist: {playlistTitle}...')
                
                for pos, pli in enumerate(playlistsToCreate[-1]['videos']):
                    pli['position'] = pos
                previousSplitPoint = splitPoint + 1
        else:
            print(f'\tPlaylist [{game}][{len(playListItems)}] skipped...')

    return playlistsToCreate

def createArchivalPlaylists(playlistData, playlistsToCreate):
    for game in playlistData:
        print('Creating new Archival Playlists for Game [{game}]...')

        print('\tFinding existing playlists...')
        for pos, newPlaylist in enumerate(playlistsToCreate):
            game = newPlaylist['playlist_game']
            archiveNum = newPlaylist['archive_num']

            if (game, str(archiveNum)) in [(p['playlist_game'], p['archive_num']) for p in playlistData[game]]:
                print('\t\tPlaylist already exists, updating entry...')
                existingPlaylist = [p for p in playlistData[game] if p['playlist_game'] == game and p['archive_num'] == str(archiveNum)][0]
                newPlaylist['playlist_id'] = existingPlaylist['playlist_id']
            else:
                ytCreatePlaylist(newPlaylist)
                playlistData[game].append(newPlaylist)

        return

def ytCreatePlaylist(newPlaylist):
    newPlaylist['videoList'] = []

    if not CONFIG.getboolean('GENERAL', 'testMode'):
        print('\t\tPlaylist Not Found: Creating playlist...')
        request = youtube.playlists().insert(
            part='snippet,status',
            body={
            'snippet': {
                'title': newPlaylist['playlist_title'],
                'description': f'{newPlaylist["playlist_game"]} episodes {newPlaylist["ep_start"]} through {newPlaylist["ep_end"]}.',
                'defaultLanguage': 'en'
            },
            'status': {
                'privacyStatus': 'public'
            }
            }
        )
        response = request.execute()
        newPlaylist['playlist_id'] = response['id']

        print('\t\tPlaylist Not Found: Adding row to playlists spreadsheet...')
        playlistWorksheet = sheet.worksheet_by_title(CONFIG['SHEET']['playlistSheetName'])

        headers = playlistWorksheet.get_values(start='A2', end='Z2', include_tailing_empty=False, returnas='matrix')[0]
        playlistWorksheet.insert_rows(playlistWorksheet.rows,
                                    values=[newPlaylist[col] if col in newPlaylist.keys() else '' for col in headers],
                                    inherit=True)
    else:
        print(f'\t\t\tSkipping API call youtube.playlists().insert(title={newPlaylist["playlist_title"]}, '\
              f'description={newPlaylist["playlist_game"]} episodes {newPlaylist["ep_start"]} through {newPlaylist["ep_end"]}.')
        
        newPlaylist['playlist_id'] = 'TO ADD'
        
        global simulatedQuota
        simulatedQuota += 50

        print('\t\t\tSkipping modification to sheet...')

    return

if __name__ == '__main__':
    print('Parsing config file...')
    CONFIG = configparser.ConfigParser()
    CONFIG.read('config.ini')

    gc = pygsheets.authorize(service_file=CONFIG['SHEET']['serviceToken'])
    sheet = gc.open_by_key(CONFIG['SHEET']['id'])

    youtube = AuthManager.get_authenticated_service('playlist-manager',
                                                    clientSecretFile=CONFIG['AUTHENTICATION']['clientSecret'],
                                                    scopes=['https://www.googleapis.com/auth/youtube.force-ssl'],
                                                    config=CONFIG)

    main()