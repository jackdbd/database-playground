select m1.title, m1.director
from Movie m1
inner join
(
  select m.director, count(*) totMovies
  from Movie m
  group by m.director
  having totMovies > 1
) as t1 on m1.director = t1.director;