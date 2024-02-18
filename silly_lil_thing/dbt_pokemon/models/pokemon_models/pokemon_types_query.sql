select *

from {{ source('main', 'pokemon_types') }}