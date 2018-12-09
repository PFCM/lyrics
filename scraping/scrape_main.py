"""Main entry point for scraping lyrics pages"""
import asyncio
import functools
import os

import aiofiles
import aiohttp
import click
from bs4 import BeautifulSoup


async def fetch(session, url):
    """grab a webpage, return text as a string"""
    async with session.get(url) as response:
        while True:
            try:
                return await response.text()
            except aiohttp.client_exceptions.ClientOSError as err:
                if '54' in str(err):
                    print('connection reset, backing off')
                    await asyncio.sleep(5)
                    continue
                else:
                    raise


async def get_and_write_lyrics_com_song(session, directory, song_name,
                                        song_fragment):
    """get a song and write it into a text file"""
    text = await get_lyrics_com_song(session, song_fragment)
    await write(os.path.join(directory, song_name + '.txt'), text)
    print('---written {}'.format(song_name))


async def get_lyrics_com_song(session, song_fragment):
    """get the text for a song from a relative link"""
    url = 'http://lyrics.com' + song_fragment
    page = BeautifulSoup(await fetch(session, url), features='html5lib')
    return page.find('pre', {'id': 'lyric-body-text'}).get_text()


async def write(path, contents):
    """write a file"""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    async with aiofiles.open(path, 'w') as outfile:
        await outfile.write(contents)


async def get_lyrics_com_songs_for_album(session, album):
    """get all the songs in an album and links"""
    url = 'http://lyrics.com/album/' + album
    text = await fetch(session, url)
    page = BeautifulSoup(text, features='html5lib')
    urls = {}
    for row in page.find('tbody').find_all('tr'):
        if row.contents[1].strong:
            name = row.contents[1].strong.a.get_text()
            link = row.contents[1].strong.a.get('href')
            urls[name] = link
        else:
            print('song "{}" does not have lyrics'.format(
                row.contents[1].get_text()))
    return urls


async def get_lyrics_com_albums_for_artist(client, artist):
    """get all the album names and links from an artist page"""
    url = 'http://lyrics.com/artist/' + artist
    page = BeautifulSoup(await fetch(client, url), features='html5lib')
    album_divs = page.find('div', {
        'id': 'content-body'
    }).find('div', {
        'class': 'tdata-ext'
    }).find_all('div', {'class': 'clearfix'})
    albums = {}
    for div in album_divs:
        name = div.h3.a.get_text()
        url = div.h3.a.get('href').split('/')[-1]
        albums[name] = url
    return albums


async def get_lyrics_com_album(session, album, outputdir='.'):
    """get lyrics for a whole album"""
    song_urls = await get_lyrics_com_songs_for_album(session, album)
    song_awaitables = [
        get_and_write_lyrics_com_song(session, outputdir, name, url)
        for name, url in song_urls.items()
    ]
    await asyncio.gather(*song_awaitables)


async def get_lyrics_com_artist(artist, outputdir='.'):
    """get all albums and all songs for an artist"""
    async with aiohttp.ClientSession() as client:
        albums = await get_lyrics_com_albums_for_artist(client, artist)
        album_awaitables = [
            get_lyrics_com_album(client, album, os.path.join(outputdir, name))
            for name, album in albums.items()
        ]
        await asyncio.gather(*album_awaitables)


@click.command()
@click.option(
    '--artist',
    '-a',
    help='artist identifier, differs depending on the website',
    default='The-Sisters-Of-Mercy')
@click.option(
    '--site', '-s', type=click.Choice(['lyrics.com']), default='lyrics.com')
@click.option(
    '--output',
    '-o',
    help='path to directory in which to store results',
    default='data/The-Sisters-Of-Mercy')
def main(artist, site, output):
    """kick it off"""
    loop = asyncio.get_event_loop()
    if site == 'lyrics.com':
        run_fn = functools.partial(
            get_lyrics_com_artist, artist=artist, outputdir=output)

    loop.run_until_complete(run_fn())


if __name__ == '__main__':
    main()
