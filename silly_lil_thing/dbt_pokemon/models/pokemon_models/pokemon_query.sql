select *

from {{ source('main', 'pokemon') }}