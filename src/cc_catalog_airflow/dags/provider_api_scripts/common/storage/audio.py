from collections import namedtuple
import logging

from common.storage import util
from common.storage import columns
from common.storage import image

logger = logging.getLogger(__name__)

_AUDIO_TSV_COLUMNS = [
    # The order of this list maps to the order of the columns in the TSV.
    columns.StringColumn(
        name='foreign_identifier',  required=False, size=3000, truncate=False
    ),
    columns.URLColumn(
        name='foreign_landing_url', required=True,  size=1000
    ),
    columns.URLColumn(
        name='audio_url',           required=True,  size=3000
    ),
    columns.StringColumn(
        name='file_format',         required=False, size=25,   truncate=True
    ),
    columns.URLColumn(
        name='thumbnail_url',       required=False, size=3000
    ),
    columns.IntegerColumn(
        name='filesize',            required=False
    ),
    columns.IntegerColumn(
        name='duration',            required=False
    ),
    columns.IntegerColumn(
        name='samplerate',          required=False
    ),
    columns.IntegerColumn(
        name='bitdepth',            required=False
    ),
    columns.IntegerColumn(
        name='channels',            required=False
    ),
    columns.StringColumn(
        name='license_',            required=True,  size=50,   truncate=False
    ),
    columns.StringColumn(
        name='license_version',     required=True,  size=25,   truncate=False
    ),
    columns.StringColumn(
        name='creator',             required=False, size=2000, truncate=True
    ),
    columns.URLColumn(
        name='creator_url',         required=False, size=2000
    ),
    columns.StringColumn(
        name='title',               required=False, size=5000, truncate=True
    ),
    columns.StringColumn(
        name='album',               required=False, size=5000, truncate=True
    ),
    columns.StringColumn(
        name='genre',               required=False, size=5000, truncate=True
    ),
    columns.StringColumn(
        name='language',            required=False, size=5000, truncate=True
    ),
    columns.JSONColumn(
        name='meta_data',           required=False
    ),
    columns.JSONColumn(
        name='tags',                required=False
    ),
    columns.StringColumn(
        name='provider',            required=False, size=80,   truncate=False
    ),
    columns.StringColumn(
        name='source',              required=False, size=80,   truncate=False
    )
]


_Audio = namedtuple(
    '_Audio',
    [c.NAME for c in _AUDIO_TSV_COLUMNS]
)


class AudioStore(image.ImageStore):
    """
    A class that stores audio information from a given provider.

    Optional init arguments:
    provider:       String marking the provider in the `audio` table of the DB.
    output_file:    String giving a temporary .tsv filename (*not* the
                    full path) where the audio info should be stored.
    output_dir:     String giving a path where `output_file` should be placed.
    buffer_length:  Integer giving the maximum number of audio information rows
                    to store in memory before writing them to disk.
    """

    def __init__(
            self,
            provider=None,
            output_file=None,
            output_dir=None,
            buffer_length=100
    ):
        super().__init__(provider, output_file, output_dir, buffer_length)

    def add_item(
            self,
            foreign_landing_url=None,
            audio_url=None,
            file_format=None,
            thumbnail_url=None,
            duration=None,
            samplerate=None,
            bitdepth=None,
            channels=None,
            license_url=None,
            license_=None,
            license_version=None,
            foreign_identifier=None,
            creator=None,
            creator_url=None,
            title=None,
            album=None,
            genre=None,
            language=None,
            meta_data=None,
            raw_tags=None,
            source=None
    ):
        """
        Add information for a single audio file to the FileStore.

        Required Arguments:

        foreign_landing_url:  URL of page where the audio lives on the
                              source website.
        audio_url:            Direct link to the audio file

        Semi-Required Arguments

        license_url:      URL of the license for the audio on the
                          Creative Commons website.
        license_:         String representation of a Creative Commons
                          license.  For valid options, see
                          `storage.constants.LICENSE_PATH_MAP`
        license_version:  Version of the given license.

        Note on license arguments: These are 'semi-required' in that
        either a valid `license_url` must be given, or a valid
        `license_`, `license_version` pair must be given. Otherwise, the
        audio data will be discarded.

        Optional Arguments:

        file_format:         The format of the audio (e.g. mp3, ogg)
        thumbnail_url:       Direct link to image accompanying audio
        duration:            Audio duration, in seconds.
        samplerate:          Samplerate of audio, in samples per second.
        bitdepth:            Bitdepth of audio, in bits.
        channels:            Number of channels in the audio.
        foreign_identifier:  Unique identifier for the audio on the
                             source site.
        creator:             The creator of the audio.
        creator_url:         The user page, or home page of the creator.
        title:               Title of the audio.
        album:               Name of album or collection this audio belongs to.
        genre:               The genre of audio.
        language:            Primary human language used in the audio.
        meta_data:           Dictionary of meta_data about the audio.
                             Currently, a key that we prefer to have is
                             `description`. If 'license_url' is included
                             in this dictionary, and `license_url` is
                             given as an argument, the argument will
                             replace the one given in the dictionary.
        raw_tags:            List of tags associated with the audio.
        source:              If different from the provider.  This might
                             be the case when we get information from
                             some aggregation of audio.  In this case,
                             the `source` argument gives the aggregator,
                             and the `provider` argument in the
                             AudioStore init function is the specific
                             provider of the audio.
        """
        audio = self._get_audio(
                foreign_landing_url=foreign_landing_url,
                audio_url=audio_url,
                file_format=file_format,
                thumbnail_url=thumbnail_url,
                duration=duration,
                samplerate=samplerate,
                bitdepth=bitdepth,
                channels=channels,
                license_url=license_url,
                license_=license_,
                license_version=license_version,
                foreign_identifier=foreign_identifier,
                creator=creator,
                creator_url=creator_url,
                title=title,
                album=album,
                genre=genre,
                language=language,
                meta_data=meta_data,
                raw_tags=raw_tags,
                source=source
            )
        tsv_row = self._create_tsv_row(audio)
        if tsv_row:
            self._image_buffer.append(tsv_row)
            self._total_images += 1
        if len(self._image_buffer) >= self._BUFFER_LENGTH:
            self._flush_buffer()

        # TODO: fix violation of encapsulation of ImageStore
        return self._total_images

    def _get_audio(
            self,
            foreign_identifier,
            foreign_landing_url,
            audio_url,
            file_format,
            thumbnail_url,
            duration,
            samplerate,
            bitdepth,
            channels,
            license_url,
            license_,
            license_version,
            creator,
            creator_url,
            title,
            album,
            genre,
            language,
            meta_data,
            raw_tags,
            source,
    ):
        license_, license_version = util.choose_license_and_version(
            license_url=license_url,
            license_=license_,
            license_version=license_version
        )
        source = util.get_source(source, self._PROVIDER)
        meta_data = self._enrich_meta_data(
            meta_data,
            license_url=license_url
        )
        tags = self._enrich_tags(raw_tags)

        return _Audio(
                foreign_identifier=foreign_identifier,
                foreign_landing_url=foreign_landing_url,
                audio_url=audio_url,
                file_format=file_format,
                thumbnail_url=thumbnail_url,
                duration=duration,
                samplerate=samplerate,
                bitdepth=bitdepth,
                channels=channels,
                license_=license_,
                license_version=license_version,
                filesize=None,
                creator=creator,
                creator_url=creator_url,
                title=title,
                album=album,
                genre=genre,
                language=language,
                meta_data=meta_data,
                tags=tags,
                provider=self._PROVIDER,
                source=source
            )

    def _create_tsv_row(
            self,
            audio,
            columns=_AUDIO_TSV_COLUMNS
    ):
        return super()._create_tsv_row(audio, columns)
